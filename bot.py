import asyncio
import os
import re
import uuid
from datetime import datetime
from typing import Optional, List, Tuple

import asyncpg
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command, CommandStart
from aiogram.types import Message, CallbackQuery
from aiogram.utils.keyboard import InlineKeyboardBuilder
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
ADMIN_IDS_RAW = os.getenv("ADMIN_IDS", "").strip()
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()  # Railway Postgres

CURRENCY = "MXN"

ADMIN_IDS = set()
if ADMIN_IDS_RAW:
    for x in ADMIN_IDS_RAW.split(","):
        x = x.strip()
        if x.isdigit():
            ADMIN_IDS.add(int(x))


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def money_to_cents(amount_str: str) -> Optional[int]:
    s = amount_str.strip().replace(",", ".")
    if not re.fullmatch(r"\d+(\.\d{1,2})?", s):
        return None
    if "." in s:
        a, b = s.split(".")
        b = (b + "0")[:2]
    else:
        a, b = s, "00"
    return int(a) * 100 + int(b)


def cents_to_money(cents: int) -> str:
    sign = "-" if cents < 0 else ""
    cents = abs(cents)
    return f"{sign}${cents//100}.{cents%100:02d} {CURRENCY}"


def main_menu_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="🛒 Comprar códigos", callback_data="menu:buy")
    kb.button(text="💰 Mi saldo", callback_data="menu:balance")
    kb.button(text="➕ Recargar saldo", callback_data="menu:topup")
    kb.button(text="📦 Mis compras", callback_data="menu:orders")
    kb.button(text="🆘 Soporte", callback_data="menu:support")
    kb.adjust(2, 2, 1)
    return kb.as_markup()


def products_kb(rows: List[Tuple[str, str, int]]):
    kb = InlineKeyboardBuilder()
    for sku, name, price_cents in rows:
        kb.button(text=f"{name} — {cents_to_money(int(price_cents))}", callback_data=f"buy:{sku}")
    kb.button(text="⬅️ Volver", callback_data="menu:back")
    kb.adjust(1)
    return kb.as_markup()


class DB:
    def __init__(self, pool: asyncpg.Pool):
        self.pool = pool

    async def init(self):
        async with self.pool.acquire() as conn:
            await conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                telegram_id BIGINT PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                balance_cents INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS products (
                sku TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                price_cents INTEGER NOT NULL,
                active BOOLEAN NOT NULL DEFAULT TRUE
            );
            CREATE TABLE IF NOT EXISTS codes (
                id BIGSERIAL PRIMARY KEY,
                sku TEXT NOT NULL REFERENCES products(sku),
                code TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'available', -- available|delivered
                created_at TEXT NOT NULL,
                delivered_at TEXT,
                buyer_telegram_id BIGINT REFERENCES users(telegram_id),
                order_id TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_codes_sku_status ON codes(sku, status);

            CREATE TABLE IF NOT EXISTS orders (
                order_id TEXT PRIMARY KEY,
                telegram_id BIGINT NOT NULL REFERENCES users(telegram_id),
                sku TEXT NOT NULL REFERENCES products(sku),
                price_cents INTEGER NOT NULL,
                status TEXT NOT NULL, -- paid_delivered|failed
                created_at TEXT NOT NULL,
                delivered_at TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_orders_user_time ON orders(telegram_id, delivered_at);

            CREATE TABLE IF NOT EXISTS balance_moves (
                id BIGSERIAL PRIMARY KEY,
                telegram_id BIGINT NOT NULL REFERENCES users(telegram_id),
                type TEXT NOT NULL, -- topup|purchase|admin_adjust
                amount_cents INTEGER NOT NULL,
                balance_before_cents INTEGER NOT NULL,
                balance_after_cents INTEGER NOT NULL,
                ref TEXT,
                created_at TEXT NOT NULL
            );
            """)

    async def upsert_user(self, telegram_id: int, username: Optional[str], first_name: Optional[str]):
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT telegram_id FROM users WHERE telegram_id=$1", telegram_id)
            if row is None:
                await conn.execute(
                    "INSERT INTO users(telegram_id, username, first_name, balance_cents, created_at) VALUES($1,$2,$3,0,$4)",
                    telegram_id, username, first_name, now_str()
                )
            else:
                await conn.execute(
                    "UPDATE users SET username=$2, first_name=$3 WHERE telegram_id=$1",
                    telegram_id, username, first_name
                )

    async def get_balance(self, telegram_id: int) -> int:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT balance_cents FROM users WHERE telegram_id=$1", telegram_id)
            return int(row["balance_cents"]) if row else 0

    async def user_id_by_username(self, username: str) -> Optional[int]:
        u = username.lstrip("@").strip()
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT telegram_id FROM users WHERE username=$1", u)
            return int(row["telegram_id"]) if row else None

    async def set_balance_with_move(self, telegram_id: int, new_balance: int, move_type: str, amount_cents: int, ref: str = ""):
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow("SELECT balance_cents FROM users WHERE telegram_id=$1 FOR UPDATE", telegram_id)
                if not row:
                    return
                before = int(row["balance_cents"])
                after = int(new_balance)
                await conn.execute("UPDATE users SET balance_cents=$2 WHERE telegram_id=$1", telegram_id, after)
                await conn.execute(
                    """INSERT INTO balance_moves(telegram_id,type,amount_cents,balance_before_cents,balance_after_cents,ref,created_at)
                       VALUES($1,$2,$3,$4,$5,$6,$7)""",
                    telegram_id, move_type, int(amount_cents), before, after, ref, now_str()
                )

    async def ensure_product(self, sku: str, name: Optional[str] = None, price_cents: Optional[int] = None):
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT sku FROM products WHERE sku=$1", sku)
            if row is None:
                await conn.execute(
                    "INSERT INTO products(sku,name,price_cents,active) VALUES($1,$2,$3,TRUE)",
                    sku, name or sku, int(price_cents or 0)
                )

    async def set_price(self, sku: str, price_cents: int):
        async with self.pool.acquire() as conn:
            await conn.execute("UPDATE products SET price_cents=$2 WHERE sku=$1", sku, int(price_cents))

    async def set_name(self, sku: str, name: str):
        async with self.pool.acquire() as conn:
            await conn.execute("UPDATE products SET name=$2 WHERE sku=$1", sku, name)

    async def set_active(self, sku: str, active: bool):
        async with self.pool.acquire() as conn:
            await conn.execute("UPDATE products SET active=$2 WHERE sku=$1", sku, bool(active))

    async def list_active_products(self) -> List[Tuple[str, str, int]]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("SELECT sku, name, price_cents FROM products WHERE active=TRUE ORDER BY name ASC")
            return [(r["sku"], r["name"], int(r["price_cents"])) for r in rows]

    async def stock_for_sku(self, sku: str) -> int:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT COUNT(*) AS c FROM codes WHERE sku=$1 AND status='available'", sku)
            return int(row["c"]) if row else 0

    async def add_codes(self, sku: str, codes: List[str]) -> int:
        await self.ensure_product(sku, name=sku, price_cents=0)
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                n = 0
                for c in codes:
                    c = c.strip()
                    if not c:
                        continue
                    await conn.execute(
                        "INSERT INTO codes(sku, code, status, created_at) VALUES($1,$2,'available',$3)",
                        sku, c, now_str()
                    )
                    n += 1
                return n

    async def deliver_purchase(self, telegram_id: int, sku: str) -> Tuple[bool, str]:
        # Load product
        async with self.pool.acquire() as conn:
            prod = await conn.fetchrow("SELECT name, price_cents, active FROM products WHERE sku=$1", sku)
        if not prod:
            return False, "Producto no existe."
        if not bool(prod["active"]):
            return False, "Producto no está disponible."

        name = prod["name"]
        price_cents = int(prod["price_cents"])

        async with self.pool.acquire() as conn:
            async with conn.transaction():
                # Lock user balance
                u = await conn.fetchrow("SELECT balance_cents FROM users WHERE telegram_id=$1 FOR UPDATE", telegram_id)
                if not u:
                    return False, "Usuario no registrado. Usa /start."
                balance = int(u["balance_cents"])

                if balance < price_cents:
                    faltan = price_cents - balance
                    return False, f"❌ Saldo insuficiente. Te faltan {cents_to_money(faltan)}."

                # Pick 1 available code with row lock (safe under concurrency)
                code_row = await conn.fetchrow("""
                    SELECT id, code FROM codes
                    WHERE sku=$1 AND status='available'
                    ORDER BY id ASC
                    LIMIT 1
                    FOR UPDATE SKIP LOCKED
                """, sku)

                if not code_row:
                    return False, "⚠️ Por el momento no hay stock de este producto."

                code_id = int(code_row["id"])
                code_value = code_row["code"]

                order_id = uuid.uuid4().hex[:10].upper()
                delivered_at = now_str()
                new_balance = balance - price_cents

                # Mark code delivered
                await conn.execute("""
                    UPDATE codes
                    SET status='delivered', delivered_at=$2, buyer_telegram_id=$3, order_id=$4
                    WHERE id=$1
                """, code_id, delivered_at, telegram_id, order_id)

                # Create order
                await conn.execute("""
                    INSERT INTO orders(order_id, telegram_id, sku, price_cents, status, created_at, delivered_at)
                    VALUES($1,$2,$3,$4,'paid_delivered',$5,$5)
                """, order_id, telegram_id, sku, price_cents, delivered_at)

                # Update balance + move
                await conn.execute("UPDATE users SET balance_cents=$2 WHERE telegram_id=$1", telegram_id, new_balance)
                await conn.execute("""
                    INSERT INTO balance_moves(telegram_id,type,amount_cents,balance_before_cents,balance_after_cents,ref,created_at)
                    VALUES($1,'purchase',$2,$3,$4,$5,$6)
                """, telegram_id, -price_cents, balance, new_balance, f"order:{order_id}", delivered_at)

        return True, (
            f"✅ Compra confirmada\n"
            f"Producto: {name}\n"
            f"Código:\n`{code_value}`\n\n"
            f"Orden: `{order_id}`\n"
            f"Saldo restante: {cents_to_money(new_balance)}"
        )

    async def my_orders_text(self, telegram_id: int) -> str:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT order_id, sku, price_cents, delivered_at
                FROM orders
                WHERE telegram_id=$1
                ORDER BY delivered_at DESC
                LIMIT 10
            """, telegram_id)

        if not rows:
            return "Aún no tienes compras registradas."

        lines = ["📦 Tus últimas compras (máx. 10):"]
        for r in rows:
            lines.append(f"- {r['delivered_at']} | {r['sku']} | {cents_to_money(int(r['price_cents']))} | Orden {r['order_id']}")
        return "\n".join(lines)


PENDING_ADD = {}  # admin_id -> sku


async def main():
    if not BOT_TOKEN:
        raise RuntimeError("Falta BOT_TOKEN en variables de entorno.")

    if not DATABASE_URL:
        raise RuntimeError("Falta DATABASE_URL. Agrega PostgreSQL en Railway para que funcione 24/7 con datos persistentes.")

    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
    db = DB(pool)
    await db.init()

    bot = Bot(BOT_TOKEN)
    dp = Dispatcher()

    @dp.message(CommandStart())
    async def start(m: Message):
        await db.upsert_user(m.from_user.id, m.from_user.username, m.from_user.first_name)
        await m.answer(
            "👋 ¡Hola! Bienvenido.\n\n"
            "Este bot entrega códigos automáticamente.\n"
            "Usa el menú para comprar o consultar tu saldo.",
            reply_markup=main_menu_kb()
        )

    @dp.message(Command("id"))
    async def myid(m: Message):
        await db.upsert_user(m.from_user.id, m.from_user.username, m.from_user.first_name)
        await m.answer(f"Tu Telegram ID es: `{m.from_user.id}`", parse_mode="Markdown")

    @dp.callback_query(F.data.startswith("menu:"))
    async def menu(call: CallbackQuery):
        await call.answer()
        await db.upsert_user(call.from_user.id, call.from_user.username, call.from_user.first_name)
        action = call.data.split(":", 1)[1]

        if action == "buy":
            rows = await db.list_active_products()
            if not rows:
                await call.message.edit_text("⚠️ Aún no hay productos cargados.", reply_markup=main_menu_kb())
                return
            await call.message.edit_text("🛒 Elige un producto:", reply_markup=products_kb(rows))

        elif action == "balance":
            bal = await db.get_balance(call.from_user.id)
            await call.message.edit_text(f"💰 Tu saldo actual: {cents_to_money(bal)}", reply_markup=main_menu_kb())

        elif action == "topup":
            await call.message.edit_text(
                "➕ *Recargar saldo*\n\n"
                "Para recargar, contacta a soporte con:\n"
                "- Tu usuario (@usuario)\n"
                "- Monto a recargar (MXN)\n\n"
                "Cuando se confirme tu pago, tu saldo será acreditado.",
                parse_mode="Markdown",
                reply_markup=main_menu_kb()
            )

        elif action == "orders":
            txt = await db.my_orders_text(call.from_user.id)
            await call.message.edit_text(txt, reply_markup=main_menu_kb())

        elif action == "support":
            await call.message.edit_text(
                "🆘 Soporte\n\n"
                "Escríbenos por este chat y te atendemos.\n"
                "Si necesitas recarga, envía @usuario y monto MXN.",
                reply_markup=main_menu_kb()
            )

        elif action == "back":
            await call.message.edit_text("Menú principal:", reply_markup=main_menu_kb())

    @dp.callback_query(F.data.startswith("buy:"))
    async def buy(call: CallbackQuery):
        await call.answer()
        sku = call.data.split(":", 1)[1]
        ok, msg = await db.deliver_purchase(call.from_user.id, sku)
        if ok:
            await call.message.edit_text(msg, parse_mode="Markdown", reply_markup=main_menu_kb())
        else:
            await call.message.edit_text(msg, reply_markup=main_menu_kb())

    # ---------------- ADMIN COMMANDS ----------------

    @dp.message(Command("admin"))
    async def admin_help(m: Message):
        if not is_admin(m.from_user.id):
            return
        await m.answer(
            "🛠️ *Comandos admin*\n\n"
            "`/sumar @usuario 200`\n"
            "`/restar @usuario 50`\n"
            "`/saldo @usuario`\n\n"
            "`/addcodes SKU` (luego pega códigos, 1 por línea)\n"
            "`/done` (termina carga)\n\n"
            "`/stock` o `/stock SKU`\n"
            "`/precio SKU 129`\n"
            "`/nombre SKU Nombre Bonito`\n"
            "`/activar SKU` | `/desactivar SKU`\n",
            parse_mode="Markdown"
        )

    @dp.message(Command("saldo"))
    async def admin_saldo(m: Message):
        if not is_admin(m.from_user.id):
            return
        parts = m.text.split()
        if len(parts) != 2:
            await m.answer("Uso: /saldo @usuario")
            return
        uid = await db.user_id_by_username(parts[1])
        if not uid:
            await m.answer("No encontré ese usuario. Pídele que use /start primero.")
            return
        bal = await db.get_balance(uid)
        await m.answer(f"Saldo de {parts[1]}: {cents_to_money(bal)}")

    @dp.message(Command("sumar"))
    async def admin_sumar(m: Message):
        if not is_admin(m.from_user.id):
            return
        parts = m.text.split()
        if len(parts) != 3:
            await m.answer("Uso: /sumar @usuario 200")
            return
        uid = await db.user_id_by_username(parts[1])
        if not uid:
            await m.answer("No encontré ese usuario. Pídele que use /start primero.")
            return
        cents = money_to_cents(parts[2])
        if cents is None or cents <= 0:
            await m.answer("Monto inválido. Ej: 200 o 200.50")
            return
        before = await db.get_balance(uid)
        after = before + cents
        await db.set_balance_with_move(uid, after, "topup", cents, ref=f"admin:{m.from_user.id}")
        await m.answer(f"✅ Recarga aplicada a {parts[1]}. Nuevo saldo: {cents_to_money(after)}")

    @dp.message(Command("restar"))
    async def admin_restar(m: Message):
        if not is_admin(m.from_user.id):
            return
        parts = m.text.split()
        if len(parts) != 3:
            await m.answer("Uso: /restar @usuario 50")
            return
        uid = await db.user_id_by_username(parts[1])
        if not uid:
            await m.answer("No encontré ese usuario. Pídele que use /start primero.")
            return
        cents = money_to_cents(parts[2])
        if cents is None or cents <= 0:
            await m.answer("Monto inválido. Ej: 50 o 50.00")
            return
        before = await db.get_balance(uid)
        after = before - cents
        await db.set_balance_with_move(uid, after, "admin_adjust", -cents, ref=f"admin:{m.from_user.id}")
        await m.answer(f"✅ Ajuste aplicado a {parts[1]}. Nuevo saldo: {cents_to_money(after)}")

    @dp.message(Command("addcodes"))
    async def admin_addcodes(m: Message):
        if not is_admin(m.from_user.id):
            return
        parts = m.text.split(maxsplit=1)
        if len(parts) != 2:
            await m.answer("Uso: /addcodes DISNEY_1M")
            return
        sku = parts[1].strip().upper()
        PENDING_ADD[m.from_user.id] = sku
        await db.ensure_product(sku, name=sku, price_cents=0)
        await m.answer(
            f"📥 Listo. Pega los códigos para `{sku}` (uno por línea).\n"
            f"Cuando termines escribe `/done`.",
            parse_mode="Markdown"
        )

    @dp.message(Command("done"))
    async def admin_done(m: Message):
        if not is_admin(m.from_user.id):
            return
        sku = PENDING_ADD.pop(m.from_user.id, None)
        if not sku:
     
