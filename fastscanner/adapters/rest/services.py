from fastapi import Request

from fastscanner.services.indicators.service import IndicatorsService


def get_indicators_service(request: Request) -> IndicatorsService:
    return request.app.indicators
