import json
import logging
from datetime import datetime, time
from typing import Any, Dict
from uuid import uuid4

import pandas as pd
from fastapi import APIRouter, Depends, WebSocket, WebSocketDisconnect
from pydantic import BaseModel

from fastscanner.adapters.rest.services import (
    get_scanner_service,
    get_scanner_service_ws,
)
from fastscanner.pkg.clock import ClockRegistry
from fastscanner.services.scanners.ports import ScannerParams
from fastscanner.services.scanners.service import ScannerService

from .models import (
    ActionType,
    ScannerMessage,
    ScanRealtimeSubscribeRequest,
    ScanRealtimeSubscribeResponse,
    ScanRealtimeUnsubscribeRequest,
    ScanRequest,
    ScanResponse,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/scanners", tags=["scanner"])


class WebSocketScannerHandler:
    def __init__(self, scanner_id: str, websocket: WebSocket):
        self._websocket = websocket
        self._scanner_id = scanner_id

    async def handle(self, symbol: str, new_row: pd.Series, passed: bool) -> pd.Series:
        if not passed:
            return new_row

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
    websocket: WebSocket, service: ScannerService = Depends(get_scanner_service_ws)
):
    await websocket.accept()

    data = await websocket.receive_text()
    request = ScanRealtimeSubscribeRequest.model_validate_json(data)
    processed_params = _parse_known_parameters(request.params)
    scanner_params = ScannerParams(type_=request.type, params=processed_params)
    handler = WebSocketScannerHandler(
        scanner_id=request.scanner_id, websocket=websocket
    )

    await service.subscribe_realtime(
        scanner_id=request.scanner_id,
        params=scanner_params,
        handler=handler,
        freq=request.freq,
    )

    try:
        response = ScanRealtimeSubscribeResponse(scanner_id=request.scanner_id)
        await websocket.send_text(response.model_dump_json())

        logger.info(
            f"Started scanner with ID: {request.scanner_id}, Type: {request.type}"
        )
        while True:
            msg = await websocket.receive_text()
            msg_data = json.loads(msg)
            if msg_data.get("action") == ActionType.UNSUBSCRIBE:
                break
    except WebSocketDisconnect:
        logger.info("WebSocket disconnected")
    finally:
        await service.unsubscribe_realtime(request.scanner_id)
        logger.info(f"Unsubscribed scanner {request.scanner_id}")


@router.post("/{scanner_type}/scans")
async def scan(
    request: ScanRequest, service: ScannerService = Depends(get_scanner_service)
) -> ScanResponse:

    result = await service.scan_all(
        scanner_type=request.type,
        params=request.params,
        start=request.start,
        end=request.end,
        freq=request.freq,
    )

    return ScanResponse(
        results=result.results,
        scanner_type=result.scanner_type,
    )
