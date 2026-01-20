from fastapi import Request, WebSocket

from fastscanner.services.indicators.service import IndicatorsService
from fastscanner.services.scanners.service import ScannerService


def get_indicators_service(request: Request) -> IndicatorsService:
    return request.state.indicators


def get_indicators_service_ws(websocket: WebSocket) -> IndicatorsService:
    return websocket.state.indicators


def get_scanner_service(request: Request) -> ScannerService:
    return request.state.scanner


def get_scanner_service_ws(websocket: WebSocket) -> ScannerService:
    return websocket.state.scanner
