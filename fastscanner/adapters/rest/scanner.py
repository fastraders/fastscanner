import logging
from datetime import datetime, time, date
from typing import Any, Dict, List

import pandas as pd
from fastapi import APIRouter, Depends, WebSocket, WebSocketDisconnect
from pydantic import BaseModel

from fastscanner.adapters.candle.partitioned_csv import PartitionedCSVCandlesProvider
from fastscanner.adapters.candle.polygon import PolygonCandlesProvider
from fastscanner.adapters.realtime.redis_channel import RedisChannel
from fastscanner.adapters.rest.services import get_scanner_service
from fastscanner.pkg import config
from fastscanner.pkg.clock import ClockRegistry, LocalClock
from fastscanner.services.scanners.ports import ScannerParams, SymbolsProvider
from fastscanner.services.scanners.service import ScannerService, SubscriptionHandler

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/scanners", tags=["scanner"])


class ScannerRequest(BaseModel):
    type: str
    params: Dict[str, Any]


class ScannerResponse(BaseModel):
    scanner_id: str


class ScanRequest(BaseModel):
    start: date
    end: date
    freq: str
    type: str
    params: Dict[str, Any]


class ScanResponse(BaseModel):
    results: List[Dict[str, Any]]
    total_symbols: int
    scanner_type: str


class ScannerMessage(BaseModel):
    symbol: str
    scan_time: str
    scanner_id: str
    candle: Dict[str, Any]


class WebSocketScannerHandler:
    def __init__(self, websocket: WebSocket):
        self._websocket = websocket
        self._scanner_id = ""

    async def handle(self, symbol: str, new_row: pd.Series, passed: bool) -> pd.Series:
        if not passed:
            return new_row

        scan_time = datetime.now().strftime("%H:%M")
        candle = new_row.to_dict()

        scan_time = new_row.name.strftime("%H:%M")  # type: ignore
        message = ScannerMessage(
            symbol=symbol,
            scan_time=scan_time,
            scanner_id=self._scanner_id,
            candle=candle,
        )

        await self._send_message(message)

        return new_row

    def set_scanner_id(self, scanner_id: str):
        self._scanner_id = scanner_id

    async def _send_message(self, message: ScannerMessage):
        try:
            message_json = message.model_dump_json()
            await self._websocket.send_text(message_json)
        except Exception as e:
            logger.error(f"Failed to send websocket message: {e}")


def _parse_known_parameters(params: Dict[str, Any]) -> Dict[str, Any]:
    """Parse known parameters and convert them to appropriate types."""
    processed_params = params.copy()

    if "start_time" in processed_params:
        processed_params["start_time"] = time.fromisoformat(
            processed_params["start_time"]
        )

    if "end_time" in processed_params:
        processed_params["end_time"] = time.fromisoformat(processed_params["end_time"])

    return processed_params


@router.websocket("")
async def websocket_realtime_scanner(
    websocket: WebSocket, service: ScannerService = Depends(get_scanner_service)
):
    await websocket.accept()
    scanner_id = None

    data = await websocket.receive_text()
    scanner_request = ScannerRequest.model_validate_json(data)
    processed_params = _parse_known_parameters(scanner_request.params)
    scanner_params = ScannerParams(type_=scanner_request.type, params=processed_params)
    handler = WebSocketScannerHandler(websocket)

    scanner_id = await service.subscribe_realtime(
        params=scanner_params, handler=handler, freq=processed_params["freq"]
    )

    response = ScannerResponse(scanner_id=scanner_id)
    await websocket.send_text(response.model_dump_json())

    logger.info(f"Started scanner with ID: {scanner_id}, Type: {scanner_request.type}")

    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        logger.info(f"WebSocket disconnected for scanner {scanner_id}")
    finally:
        if scanner_id:
            await service.unsubscribe_realtime(scanner_id)
            logger.info(f"Unsubscribed scanner {scanner_id}")


@router.post("/scan")
async def scan(
    request: ScanRequest, service: ScannerService = Depends(get_scanner_service)
) -> ScanResponse:
    processed_params = _parse_known_parameters(request.params)

    result = await service.scan_all(
        scanner_type=request.type,
        params=processed_params,
        start=request.start,
        end=request.end,
        freq=request.freq,
    )

    return ScanResponse(
        results=result.results,
        total_symbols=result.total_symbols,
        scanner_type=result.scanner_type,
    )
