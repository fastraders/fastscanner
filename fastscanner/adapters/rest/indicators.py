import json
import os
from datetime import date
from typing import Annotated, Any
from uuid import NAMESPACE_DNS, UUID, uuid5

from fastapi import APIRouter, Depends, Request, status
from pydantic import BaseModel

from fastscanner.pkg import config
from fastscanner.services.indicators.service import IndicatorParams as Params
from fastscanner.services.indicators.service import IndicatorsService

from .services import get_indicators_service

router = APIRouter(prefix="/indicators", tags=["indicators"])


class IndicatorsParams(BaseModel):
    type: str
    params: dict[str, Any]


class IndicatorsCalculate(BaseModel):
    symbol: str
    start: date
    end: date
    freq: str
    indicators: list[IndicatorsParams]


@router.post("/calculate", status_code=status.HTTP_200_OK)
async def calculate(
    indicators_calculate: IndicatorsCalculate,
    service: Annotated[IndicatorsService, Depends(get_indicators_service)],
    request: Request,
) -> str:
    body = await request.body()
    calculation_id = uuid5(NAMESPACE_DNS, body.decode("utf-8"))
    path = os.path.join(
        config.INDICATORS_CALCULATE_RESULTS_DIR, f"{calculation_id}.csv"
    )
    if os.path.exists(path):
        return path

    df = service.calculate(
        indicators_calculate.symbol,
        indicators_calculate.start,
        indicators_calculate.end,
        indicators_calculate.freq,
        [Params(i.type, i.params) for i in indicators_calculate.indicators],
    )

    df.tz_convert("utc").tz_convert(None).reset_index().to_csv(path, index=False)
    return path
