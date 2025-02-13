from fastapi import FastAPI, HTTPException
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, JSON
import requests
from typing import List, Dict, Any
from tenacity import retry, stop_after_attempt, wait_fixed
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

# Создание таблиц
Base.metadata.create_all(bind=engine)

# FastAPI приложение
app = FastAPI()

# Функция для получения данных от API с повторными попытками
@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))  # 3 попытки с задержкой 2 секунды
def fetch_data(p_index_id: int, p_period_id: int, p_terms: str, p_term_id: int, p_dicIds: str, idx: int, p_parent_id: str = "") -> List[Dict[str, Any]]:
    url = "http://taldau.stat.gov.kz/ru/Api/GetIndexTreeData"
    params = {
        "p_measure_id": 1,
        "p_index_id": p_index_id,
        "p_period_id": p_period_id,
        "p_terms": p_terms,
        "p_term_id": p_term_id,
        "p_dicIds": p_dicIds,
        "idx": idx,
        "p_parent_id": p_parent_id
    }
    response = requests.get(url, params=params)
    if response.status_code != 200:
        raise HTTPException(status_code=response.status_code, detail="Failed to fetch data")
    return response.json()

# Функция для сохранения данных в базу
def save_data(db: Session, 
              p_index_id: int, 
              p_period_id: int, 
              p_terms: str, 
              p_term_id: int, 
              p_dicIds: str, 
              idx: int, 
              p_parent_id: str, 
              response_data: List[Dict[str, Any]]):
    for item in response_data:
        db_item = IndexTreeData(
            id=item.get("id"),
            p_index_id=p_index_id,
            p_period_id=p_period_id,
            p_terms=p_terms,
            p_term_id=p_term_id,
            p_dicIds=p_dicIds,
            idx=idx,
            p_parent_id=p_parent_id,
            response_data=item
        )
        db.add(db_item)
    db.commit()

# Рекурсивная функция для получения и сохранения данных
def recursive_fetch_and_save(db: Session, p_index_id: int, p_period_id: int, p_terms: str, p_term_id: int, p_dicIds: str, idx: int, p_parent_id: str = ""):
    try:
        data = fetch_data(p_index_id, p_period_id, p_terms, p_term_id, p_dicIds, idx, p_parent_id)
        save_data(db, p_index_id, p_period_id, p_terms, p_term_id, p_dicIds, idx, p_parent_id, data)
        for item in data:
            if item["leaf"] == "false":
                recursive_fetch_and_save(db, p_index_id, p_period_id, p_terms, p_term_id, p_dicIds, idx, item["id"])
    except Exception as e:
        print(f"Error fetching or saving data: {e}")
        raise

# Эндпоинт FastAPI
@app.post("/fetch-and-save/")
def fetch_and_save(p_index_id: int, p_period_id: int, p_terms: str, p_term_id: int, p_dicIds: str, idx: int):
    db = SessionLocal()
    try:
        recursive_fetch_and_save(db, p_index_id, p_period_id, p_terms, p_term_id, p_dicIds, idx)
    except Exception as e:
        db.rollback()  # Откат транзакции в случае ошибки
        raise HTTPException(status_code=503, detail=f"Service Unavailable: {e}")
    finally:
        db.close()
    return {"message": "Data fetched and saved successfully"}