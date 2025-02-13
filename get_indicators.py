from fastapi import APIRouter, HTTPException
from sqlalchemy import select, Table, Column, Integer, String, MetaData
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from constants import USER, PASSWORD, HOST, DB

DATABASE_URL = f"postgresql+asyncpg://{USER}:{PASSWORD}@{HOST}/{DB}"  # Замените на свои данные
engine = create_async_engine(DATABASE_URL, future=True, echo=True)
async_session = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

# Определение таблицы indicators
metadata = MetaData()
indicators_table = Table(
    "indicators",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("name", String, nullable=False)
)

router = APIRouter()

@router.get(
    "/get_indicators",
    tags=["Battle"],
    summary="Показатели в локальной БД",
    description="Показатели в локальной БД"
    )
async def get_indicators():
    async with async_session() as session:
        async with session.begin():
            result = await session.execute(select(indicators_table))
            indicators = result.fetchall()
            if not indicators:
                raise HTTPException(status_code=404, detail="No indicators found")
            return [{"id": row.id, "name": row.name} for row in indicators]