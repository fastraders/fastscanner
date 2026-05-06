import json
import logging
import time as time_clock
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
from fastscanner.pkg.candle import Candle
from fastscanner.pkg.clock import ClockRegistry
from fastscanner.pkg.observability import metrics
from fastscanner.services.exceptions import UnsubscribeSignal
from fastscanner.services.scanners.ports import ScannerParams
from fastscanner.services.scanners.service import ScannerService

from .init import init
from .models import (
    ActionType,
    ScannerMessage,
    ScanRealtimeSubscribeRequest,
    ScanRealtimeSubscribeResponse,
    ScanRealtimeUnsubscribeRequest,
    ScanRequest,
    ScanResponse,
)

WS_ENDPOINT = "scanner"

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/scanners", tags=["scanner"])


class WebSocketScannerHandler:
    def __init__(self, scanner_id: str, websocket: WebSocket, freq: str):
        self._websocket = websocket
        self._scanner_id = scanner_id
        self._freq = freq
        try:
            self._freq_seconds = pd.Timedelta(freq).total_seconds()
        except (ValueError, TypeError):
            self._freq_seconds = 0.0

    async def handle(self, symbol: str, new_row: Candle, passed: bool) -> Candle:
        if not passed:
            return new_row

        candle = dict(new_row)
        scan_time = new_row.timestamp.strftime("%H:%M")
        message = ScannerMessage(
            symbol=symbol,
            scan_time=scan_time,
            scanner_id=self._scanner_id,
            candle=candle,
        )

        metrics.ws_scanner_match(self._scanner_id, self._freq)
        await self._send_message(message, candle_ts=new_row.timestamp)

        return new_row

    async def _send_message(
        self, message: ScannerMessage, candle_ts: pd.Timestamp | None = None
    ):
        start = time_clock.perf_counter()
        result = "ok"
        try:
            message_json = message.model_dump_json()
            await self._websocket.send_text(message_json)
        except WebSocketDisconnect:
            result = "disconnect"
            logger.info("WebSocket disconnected")
            raise UnsubscribeSignal("WebSocket disconnected")
        except Exception as e:
            result = "error"
            logger.error(f"Failed to send websocket message: {e}")
        finally:
            elapsed = time_clock.perf_counter() - start
            metrics.ws_message_pushed(WS_ENDPOINT, result, elapsed)
            if result == "ok" and candle_ts is not None and self._freq_seconds > 0:
                delay = (
                    time_clock.time() - candle_ts.timestamp() - self._freq_seconds
                )
                if delay >= 0:
                    metrics.ws_candle_to_client(WS_ENDPOINT, self._freq, delay)


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
    metrics.ws_connection_opened(WS_ENDPOINT)
    close_reason = "client_disconnect"
    subscribed = False
    scanner_id: str | None = None

    try:
        try:
            data = await websocket.receive_text()
            request = ScanRealtimeSubscribeRequest.model_validate_json(data)
        except WebSocketDisconnect:
            close_reason = "client_disconnect"
            return
        except Exception as e:
            close_reason = "handshake_invalid"
            metrics.ws_subscribe_error(WS_ENDPOINT, "validation")
            logger.exception(e)
            return

        processed_params = _parse_known_parameters(request.params)
        scanner_params = ScannerParams(type_=request.type, params=processed_params)
        handler = WebSocketScannerHandler(
            scanner_id=request.scanner_id, websocket=websocket, freq=request.freq
        )
        scanner_id = request.scanner_id

        sub_start = time_clock.perf_counter()
        try:
            await service.subscribe_realtime(
                scanner_id=request.scanner_id,
                params=scanner_params,
                handler=handler,
                freq=request.freq,
            )
        except Exception as e:
            elapsed = time_clock.perf_counter() - sub_start
            metrics.ws_subscribe_latency(WS_ENDPOINT, "error", elapsed)
            metrics.ws_subscribe_error(WS_ENDPOINT, "service_error")
            close_reason = "subscribe_failed"
            logger.exception(e)
            return
        elapsed = time_clock.perf_counter() - sub_start
        metrics.ws_subscribe_latency(WS_ENDPOINT, "ok", elapsed)
        subscribed = True

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
                    close_reason = "unsubscribe"
                    break
        except WebSocketDisconnect:
            close_reason = "client_disconnect"
            logger.info("WebSocket disconnected")
    finally:
        if subscribed and scanner_id is not None:
            await service.unsubscribe_realtime(scanner_id)
            logger.info(f"Unsubscribed scanner {scanner_id}")
        metrics.ws_connection_closed(WS_ENDPOINT, close_reason)


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
        init_registry=init,
    )

    return ScanResponse(
        results=result.results,
        scanner_type=result.scanner_type,
    )
