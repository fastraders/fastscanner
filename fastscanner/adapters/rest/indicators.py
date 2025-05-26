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
    indicators_calculate: list[IndicatorsCalculate],
    service: Annotated[IndicatorsService, Depends(get_indicators_service)],
    request: Request,
) -> list[str]:
    paths: list[str] = []
    for ic in indicators_calculate:
        body = await request.body()
        body_hash = uuid5(NAMESPACE_DNS, body.decode("utf-8")).hex
        calculation_id = f"{ic.symbol}_{ic.start}_{ic.end}_{ic.freq}_{body_hash[:6]}"

        path = os.path.join(
            config.INDICATORS_CALCULATE_RESULTS_DIR,
            ic.symbol,
            f"{calculation_id}.csv",
        )
        if os.path.exists(path):
            paths.append(path)
            continue

        os.makedirs(os.path.dirname(path), exist_ok=True)

        df = await service.calculate_from_params(
            ic.symbol,
            ic.start,
            ic.end,
            ic.freq,
            [Params(i.type, i.params) for i in ic.indicators],
        )

        df.tz_convert("utc").tz_convert(None).reset_index().to_csv(path, index=False)
    return paths
