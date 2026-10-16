import asyncio

from sqlalchemy import text
from app.database import engine


async def main():
    try:
        async with engine.connect() as connection:
            result = await connection.execute(
                text("SELECT version(), current_database()")
            )
            version, database = result.one()

            print("Connected to Neon PostgreSQL successfully")
            print(f"Database: {database}")
            print(f"Version: {version}")

    finally:
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())