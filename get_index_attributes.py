from fastapi import APIRouter, Query
from constants import BASE_URL, RETRIES
from fastapi.responses import JSONResponse
import time
import httpx

router = APIRouter()

@router.get(
    "/get_index_attributes",
    tags=["Battle"],
    summary="5. Метод получения информации о показателе GetIndexAttributes",
    description="5. Метод получения информации о показателе GetIndexAttributes"
    )
async def get_index_attributes(
    indexId: int = Query(..., alias="indexId", description="Идентификатор показателя"),
    periodId: int = Query(..., alias="periodId", description="Идентификатор типа периода (из запроса GetPeriodList)")
):
    url = f"{BASE_URL}/GetIndexAttributes"
    params = {
        "periodId": periodId,
        "measureID": "1",
        "measureKFC": "1",
        "indexId": indexId
    }
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
            return JSONResponse(
                status_code=exc.response.status_code,
                content={"detail": f"Ошибка запроса: {exc.response.text}"}
            )
        except httpx.RequestError as exc:
            if attempt < RETRIES - 1:
                time.sleep(2)
                continue
            return JSONResponse(
                status_code=500,
                content={"detail": f"Ошибка соединения: {exc}"}
            )
    return True