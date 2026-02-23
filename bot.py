import asyncio
import os
import uuid
from datetime import datetime

import asyncpg
from aiogram import Bot, Dispatcher
from aiogram.filters import Command, CommandStart
from aiogram.types import Message

BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
ADMIN_IDS = set(int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip().isdigit())


def is_admin(user_id: int):
    return user_id in ADMIN_IDS


async def main():
    if not BOT_TOKEN:
        raise RuntimeError("Falta BOT_TOKEN")

    if not DATABASE_URL:
        raise RuntimeError("Falta DATABASE_URL")

    pool = await asyncpg.create_pool(DATABASE_URL)
    bot = Bot(BOT_TOKEN)
    dp = Dispatcher()

    async with pool.acquire() as conn:
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            telegram_id BIGINT PRIMARY KEY,
            balance INTEGER DEFAULT 0
        )
        """)

    @dp.message(CommandStart())
    async def start(m: Message):
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO users (telegram_id) VALUES ($1) ON CONFLICT DO NOTHING",
                m.from_user.id
            )
        await m.answer("👋 Bot activo correctamente")

    @dp.message(Command("admin"))
    async def admin_panel(m: Message):
        if not is_admin(m.from_user.id):
            return
        await m.answer("🛠 Panel admin funcionando")

    print("✅ Bot iniciado correctamente")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
