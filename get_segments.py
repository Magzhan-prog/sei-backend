from fastapi import APIRouter, Query
from constants import BASE_URL, RETRIES
from fastapi.responses import JSONResponse
import time
import httpx

router = APIRouter()

def transform_data(data):
    for item in data:
        # Заменяем " + " на ", " в dicId и dicClassId
        item["id"] = item["termIds"]
        item["name"] = item["names"]
        item["dicId"] = item["dicId"].replace(" + ", ",")
        item["mas_names"] = [
            {"id": term_id.strip(), "name": name.strip()}
            for term_id, name in zip(item["termIds"].split(","), item["names"].split(" + "))
        ]

    return data

@router.get(
    "/get_segments",
    tags=["Battle"],
    summary="2. Информация о разрезностях GetSegmentList",
    description="2. Информация о разрезностях GetSegmentList"
    )
async def get_segments(
    indexId: int = Query(..., alias="indexId", description="Идентификатор показателя"),
    periodId: int = Query(..., alias="periodId", description="Идентификатор типа периода (из запроса GetPeriodList)")
):
    url = f"{BASE_URL}/GetSegmentList"
    params = {
        "indexId": indexId,
        "periodId": periodId,
    }
    for attempt in range(RETRIES):
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(url, params=params)
                response.raise_for_status()
                return transform_data(response.json())
                
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