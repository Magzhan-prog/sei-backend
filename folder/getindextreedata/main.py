from fastapi import FastAPI, Query, HTTPException
from sqlalchemy import and_
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, Integer, String, JSON
from constants import USER, PASSWORD, HOST, DB

# Настройка базы данных
DATABASE_URL = f"postgresql://{USER}:{PASSWORD}@{HOST}/{DB}"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

# Модель таблицы
class IndexTreeData(Base):
    __tablename__ = "index_tree_data"

    id = Column(String, primary_key=True)
    p_index_id = Column(Integer, primary_key=True)
    p_period_id = Column(Integer, primary_key=True)
    p_terms = Column(String, primary_key=True)
    p_term_id = Column(Integer, primary_key=True)
    p_dicIds = Column(String, primary_key=True)
    idx = Column(Integer, primary_key=True)
    p_parent_id = Column(String, primary_key=True)
    response_data = Column(JSON)

app = FastAPI()

# Эндпоинт для получения данных
@app.get("/get-data/")
def get_data(
    p_index_id: int,
    p_period_id: int,
    p_terms: str,
    p_term_id: int,
    p_dicIds: str,
    idx: int,
    p_parent_id: str = Query(default="", description="Идентификатор родительского элемента")
):
    db = SessionLocal()
    try:
        # Формируем фильтр для запроса
        filters = [
            IndexTreeData.p_index_id == p_index_id,
            IndexTreeData.p_period_id == p_period_id,
            IndexTreeData.p_terms == p_terms,
            IndexTreeData.p_term_id == p_term_id,
            IndexTreeData.p_dicIds == p_dicIds,
            IndexTreeData.idx == idx
        ]
        
        # Если p_parent_id не пустой, добавляем его в фильтр
        if p_parent_id:
            filters.append(IndexTreeData.p_parent_id == p_parent_id)
        
        # Выполняем запрос к базе данных
        results = db.query(IndexTreeData).filter(and_(*filters)).all()
        
        # Если данные не найдены, возвращаем ошибку 404
        if not results:
            raise HTTPException(status_code=404, detail="Data not found")
        
        # Формируем ответ
        if p_parent_id:
            # Если p_parent_id не пустой, возвращаем список JSON-ответов
            return [item.response_data for item in results]
        else:
            # Если p_parent_id пустой, возвращаем один JSON-ответ
            return results[0].response_data
    finally:
        db.close()