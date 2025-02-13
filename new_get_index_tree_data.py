from fastapi import APIRouter, Query, HTTPException
from constants import BASE_URL, RETRIES
import time
import httpx

router = APIRouter()

async def fetch_data(params):
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

        transformed_data.append(region_dict)

    return transformed_data

@router.get(
    "/new_get_index_tree_data",
    tags=["Battle"],
    summary="1. Данные показателя GetIndexTreeData",
    description="1. Данные показателя GetIndexTreeData"
)
async def new_get_index_tree_data(
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
    Получает данные показателя GetIndexTreeData с помощью API.
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

    tree = await fetch_data(params)
    date = await build_data(data_params)
    return transform_data(tree, date)