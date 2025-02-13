from fastapi import APIRouter, Query
from constants import BASE_URL, RETRIES
from fastapi.responses import JSONResponse
import time
import httpx

router = APIRouter()

@router.get(
    "/get_periods",
    tags=["Battle"],
    summary="3. Информация о доступных типов периодов GetPeriodList",
    description="3. Информация о доступных типов периодов GetPeriodList"
    )
async def get_periods(
    indexId: int = Query(..., alias="indexId", description="Идентификатор показателя")
):
    url = f"{BASE_URL}/GetPeriodList"
    params = {
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