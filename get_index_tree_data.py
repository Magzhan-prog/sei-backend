from fastapi import APIRouter, Query, HTTPException
from constants import BASE_URL, RETRIES
import time
import httpx
from sqlalchemy import and_
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, Integer, String, JSON
from constants import USER, PASSWORD, HOST, DB
import logging

router = APIRouter()

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

async def fetch_data(params):
    db = SessionLocal()
    try:
        print(params)
        # Формируем фильтр для запроса
        filters = [
            IndexTreeData.p_index_id == params["p_index_id"],
            IndexTreeData.p_period_id == params["p_period_id"],
            IndexTreeData.p_terms == params["p_terms"],
            IndexTreeData.p_term_id == params["p_term_id"],
            IndexTreeData.p_dicIds == params["p_dicIds"],
            IndexTreeData.idx == params["idx"]
        ]
        
        # Если p_parent_id не пустой, добавляем его в фильтр
        if params["p_parent_id"]!='':
            filters.append(IndexTreeData.p_parent_id == params["p_parent_id"])
        
        # Выполняем запрос к базе данных
        results = db.query(IndexTreeData).filter(and_(*filters)).all()
        
        # Если данные не найдены, возвращаем ошибку 404
        if not results:
            raise HTTPException(status_code=404, detail="Data not found")
        
        # Формируем ответ
        if params["p_parent_id"]!='':
            # Если p_parent_id не пустой, возвращаем список JSON-ответов
            return [item.response_data for item in results]
        else:
            # Если p_parent_id пустой, возвращаем один JSON-ответ
            return results[0].response_data
    finally:
        db.close()

async def fetch_data1(params):
    """
    Выполняет запрос к API с передачей параметров и обработкой ошибок.
    """
    url = f"{BASE_URL}/GetIndexTreeData"

    for attempt in range(RETRIES):
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(url, params=params)
                response.raise_for_status()
                return response.json()
        except httpx.HTTPStatusError as exc:
            if attempt < RETRIES - 1:
                time.sleep(2)
                continue
            raise HTTPException(
                status_code=exc.response.status_code,
                detail=f"Ошибка запроса: {exc.response.text}"
            )
        except httpx.RequestError as exc:
            if attempt < RETRIES - 1:
                time.sleep(2)
                continue
            raise HTTPException(
                status_code=500,
                detail=f"Ошибка соединения: {exc}"
            )
    raise HTTPException(
        status_code=500,
        detail="Превышено количество попыток запроса"
    )

async def build_data(params):
    """
    Выполняет запрос к API с передачей параметров и обработкой ошибок.
    """
    url = f"{BASE_URL}/GetIndexPeriods"

    for attempt in range(RETRIES):
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(url, params=params)
                response.raise_for_status()
                return response.json()
        except httpx.HTTPStatusError as exc:
            if attempt < RETRIES - 1:
                time.sleep(2)
                continue
            raise HTTPException(
                status_code=exc.response.status_code,
                detail=f"Ошибка запроса: {exc.response.text}"
            )
        except httpx.RequestError as exc:
            if attempt < RETRIES - 1:
                time.sleep(2)
                continue
            raise HTTPException(
                status_code=500,
                detail=f"Ошибка соединения: {exc}"
            )
    raise HTTPException(
        status_code=500,
        detail="Превышено количество попыток запроса"
    )

async def build_tree(params):
    """
    Рекурсивно собирает дерево из данных, пока все узлы не станут листовыми.
    """
    data = await fetch_data(params)
    
    # Если data — это словарь, преобразуем его в список
    if isinstance(data, dict):
        data = [data]

    result = []
    print(data)
    for item in data:
        node = {
            "id": item["id"],
            "text": item["text"],
            "leaf": item["leaf"],
        }

        # Извлекаем все поля, начинающиеся на 'y' (предположительно это даты)
        for key, value in item.items():
            if key.startswith("y") and isinstance(value, str):  # Можно добавить проверку на формат даты
                node[key] = value

        if item["leaf"] == "false":
            # Получаем дочерние элементы для текущего узла
            child_params = params.copy()
            child_params["p_parent_id"] = item["id"]
            node["children"] = await build_tree(child_params)
        result.append(node)
    
    return result

def transform_data(regions_data, date_data):
    transformed_data = []

    # Идем по регионам и их детям
    for region in regions_data:
        region_dict = {
            "id": region["id"],
            "text": region["text"],
            "leaf": region["leaf"]
        }
        
        # Добавляем данные по датам из dateList
        for i, date in enumerate(date_data["dateList"]):
            # Строим ключ, например "y122000"
            region_key = f"y{date}"
            
            # Получаем значение из исходных данных региона по ключу y122XXX
            year_value = region.get(region_key)
            if year_value:
                region_dict[date_data["periodNameList"][i]] = year_value

        children = []
        for child in region.get("children", []):
            child_dict = {
                "id": child["id"],
                "text": child["text"],
                "leaf": child["leaf"]
            }

            # Добавляем данные по датам для каждого ребенка
            for i, date in enumerate(date_data["dateList"]):
                # Строим ключ для ребенка, например "y122000"
                child_key = f"y{date}"
                
                # Получаем значение из исходных данных по ключу y122XXX для ребенка
                year_value = child.get(child_key)
                if year_value:
                    child_dict[date_data["periodNameList"][i]] = year_value

            children.append(child_dict)

        region_dict["children"] = children
        transformed_data.append(region_dict)

    return transformed_data

@router.get(
    "/get_index_tree_data",
    tags=["Battle"],
    summary="1. Данные показателя GetIndexTreeData",
    description="1. Данные показателя GetIndexTreeData"
)
async def get_index_tree_data(
    p_measure_id: int = Query(1, description="Идентификатор измерения (по умолчанию 1)"),
    p_index_id: int = Query(..., description="Идентификатор показателя"),
    p_period_id: int = Query(..., description="Идентификатор типа периода"),
    p_terms: str = Query(..., description="Список элементов, разделённых запятыми для выборки (termIds из GetSegmentList)"),
    p_term_id: int = Query(..., description="Главный элемент, по которому нужна детализация (один из p_terms)"),
    p_dicIds: str = Query(..., description="Список справочников, разделённых запятыми (dicId из GetSegmentList)"),
    idx: int = Query(..., description="Индекс разрезности (idx из GetSegmentList)"),
    p_parent_id: str = Query('', description="Идентификатор родительского элемента. Для корня оставить пустым.")
):
    """
    Получает данные показателя `GetIndexTreeData` с помощью API.
    """
    params = {
        "p_measure_id": p_measure_id,
        "p_index_id": p_index_id,
        "p_period_id": p_period_id,
        "p_terms": p_terms,
        "p_term_id": p_term_id,
        "p_dicIds": p_dicIds,
        "idx": idx,
        "p_parent_id": p_parent_id,
    }

    data_params = {
        "p_measure_id": p_measure_id,
        "p_index_id": p_index_id,
        "p_period_id": p_period_id,
        "p_terms": p_terms,
        "p_term_id": p_term_id,
        "p_dicIds": p_dicIds,
    }

    tree = await build_tree(params)
    date = await build_data(data_params)
    return transform_data(tree, date)
