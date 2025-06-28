from fastapi import Request

from fastscanner.services.indicators.service import IndicatorsService
from fastscanner.services.scanners.service import ScannerService


def get_indicators_service(request: Request) -> IndicatorsService:
    return request.app.indicators


def get_scanner_service(request: Request) -> ScannerService:
    return request.app.scanner
