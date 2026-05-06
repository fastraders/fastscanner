import json
import logging
import os
import time
from datetime import date, datetime
from enum import Enum
from io import StringIO
from typing import Annotated, Any, Dict, Union
from uuid import NAMESPACE_DNS, UUID, uuid5

import pandas as pd
from fastapi import APIRouter, Depends, Request, WebSocket, WebSocketDisconnect, status
from fastapi.websockets import WebSocketState
from pydantic import BaseModel

from fastscanner.pkg import config
from fastscanner.pkg.candle import Candle
from fastscanner.pkg.observability import metrics
from fastscanner.services.exceptions import UnsubscribeSignal
from fastscanner.services.indicators.service import IndicatorParams as Params
from fastscanner.services.indicators.service import (
    IndicatorsService,
    SubscriptionHandler,
)

from .models import ActionType, StatusType
from .services import get_indicators_service, get_indicators_service_ws

WS_ENDPOINT = "indicator"

logger = logging.getLogger(__name__)

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


class SubscriptionRequest(BaseModel):
    action: ActionType
    subscription_id: str
    symbol: str
    freq: str
    indicators: list[IndicatorsParams]


class UnsubscriptionRequest(BaseModel):
    action: ActionType
    subscription_id: str


class SubscriptionResponse(BaseModel):
    status: StatusType
    subscription_id: str
    message: str = ""


class IndicatorMessage(BaseModel):
    subscription_id: str
    symbol: str
    timestamp: datetime
    candle: Dict[str, Any]


WebSocketRequest = Union[SubscriptionRequest, UnsubscriptionRequest]


class WebSocketIndicatorHandler(SubscriptionHandler):
    def __init__(self, websocket: WebSocket, subscription_id: str, freq: str):
        self._websocket = websocket
        self._subscription_id = subscription_id
        self._freq = freq
        try:
            self._freq_seconds = pd.Timedelta(freq).total_seconds()
        except (ValueError, TypeError):
            self._freq_seconds = 0.0

    async def handle(self, symbol: str, new_row: Candle) -> Candle:
        message = IndicatorMessage(
            subscription_id=self._subscription_id,
            symbol=symbol,
            timestamp=new_row.timestamp,
            candle=dict(new_row),
        )

        await self._send_message(message, candle_ts=new_row.timestamp)
        return new_row

    async def _send_message(
        self, message: IndicatorMessage, candle_ts: pd.Timestamp | None = None
    ):
        start = time.perf_counter()
        result = "ok"
        try:
            message_json = message.model_dump_json()
            await self._websocket.send_text(message_json)
        except WebSocketDisconnect:
            result = "disconnect"
            logger.info("WebSocket disconnected")
            raise UnsubscribeSignal("WebSocket disconnected")
        except RuntimeError as re:
            result = "runtime_error"
            logger.info(f"RuntimeError: {re}")
            raise UnsubscribeSignal(f"RuntimeError: {re}")
        except Exception as e:
            result = "error"
            logger.exception(e)
        finally:
            elapsed = time.perf_counter() - start
            metrics.ws_message_pushed(WS_ENDPOINT, result, elapsed)
            if result == "ok" and candle_ts is not None and self._freq_seconds > 0:
                delay = time.time() - candle_ts.timestamp() - self._freq_seconds
                if delay >= 0:
                    metrics.ws_candle_to_client(WS_ENDPOINT, self._freq, delay)


@router.post("/calculate", status_code=status.HTTP_200_OK)
async def calculate(
    ic: IndicatorsCalculate,
    service: Annotated[IndicatorsService, Depends(get_indicators_service)],
    request: Request,
) -> str:
    df = await service.calculate_from_params(
        ic.symbol,
        ic.start,
        ic.end,
        ic.freq,
        [Params(i.type, i.params) for i in ic.indicators],
    )
    buffer = StringIO()

    df.tz_convert("utc").tz_convert(None).reset_index().to_csv(buffer, index=False)
    buffer.seek(0)
    return buffer.read()


async def _handle_subscribe(
    request: SubscriptionRequest,
    websocket: WebSocket,
    service: IndicatorsService,
    subscriptions: dict[str, tuple[str, str]],
) -> SubscriptionResponse:
    handler = WebSocketIndicatorHandler(
        websocket, request.subscription_id, request.freq
    )

    indicators_params = [Params(i.type, i.params) for i in request.indicators]

    start = time.perf_counter()
    try:
        subscription_id = await service.subscribe_realtime(
            symbol=request.symbol,
            freq=request.freq,
            indicators=indicators_params,
            handler=handler,
            _send_events=not _is_persister_subscription(request.subscription_id),
        )
    except Exception as e:
        elapsed = time.perf_counter() - start
        metrics.ws_subscribe_latency(WS_ENDPOINT, "error", elapsed)
        metrics.ws_subscribe_error(WS_ENDPOINT, "service_error")
        logger.exception(e)
        return SubscriptionResponse(
            status=StatusType.ERROR,
            subscription_id=request.subscription_id,
            message=f"Failed to subscribe to {request.symbol}: {e}",
        )

    elapsed = time.perf_counter() - start
    metrics.ws_subscribe_latency(WS_ENDPOINT, "ok", elapsed)
    subscriptions[request.subscription_id] = (request.symbol, subscription_id)

    logger.info(
        f"Added subscription {request.subscription_id} for symbol {request.symbol} on worker {os.getpid()}",
    )

    return SubscriptionResponse(
        status=StatusType.SUCCESS,
        subscription_id=request.subscription_id,
        message=f"Subscribed to {request.symbol}",
    )


async def _handle_unsubscribe(
    request: UnsubscriptionRequest,
    service: IndicatorsService,
    subscriptions: dict[str, tuple[str, str]],
) -> SubscriptionResponse:
    if request.subscription_id not in subscriptions:
        return SubscriptionResponse(
            status=StatusType.ERROR,
            subscription_id=request.subscription_id,
            message="Subscription not found",
        )
    symbol, service_subscription_id = subscriptions[request.subscription_id]
    await service.unsubscribe_realtime(
        service_subscription_id,
        _send_events=not _is_persister_subscription(request.subscription_id),
    )
    del subscriptions[request.subscription_id]

    logger.info(f"Removed subscription {request.subscription_id}")

    return SubscriptionResponse(
        status=StatusType.SUCCESS,
        subscription_id=request.subscription_id,
        message=f"Unsubscribed from {symbol}",
    )


@router.websocket("")
async def websocket_realtime_indicators(
    websocket: WebSocket,
    service: IndicatorsService = Depends(get_indicators_service_ws),
):
    await websocket.accept()
    metrics.ws_connection_opened(WS_ENDPOINT)
    subscriptions: dict[str, tuple[str, str]] = {}
    close_reason = "client_disconnect"

    data = ""
    try:
        while True:
            try:
                data = await websocket.receive_text()

                # Parse request and determine action
                request_data = json.loads(data)
                action = request_data.get("action")

                if action == ActionType.SUBSCRIBE:
                    request = SubscriptionRequest.model_validate_json(data)
                    response = await _handle_subscribe(
                        request, websocket, service, subscriptions
                    )
                    await websocket.send_text(response.model_dump_json())

                elif action == ActionType.UNSUBSCRIBE:
                    request = UnsubscriptionRequest.model_validate_json(data)
                    response = await _handle_unsubscribe(
                        request, service, subscriptions
                    )
                    await websocket.send_text(response.model_dump_json())

                else:
                    metrics.ws_subscribe_error(WS_ENDPOINT, "unknown_action")
                    response = SubscriptionResponse(
                        status=StatusType.ERROR,
                        subscription_id="",
                        message=f"Unknown action: {action}",
                    )
                    await websocket.send_text(response.model_dump_json())

            except WebSocketDisconnect:
                close_reason = "client_disconnect"
                logger.info("WebSocket disconnected")
                break
            except RuntimeError as re:
                close_reason = "runtime_error"
                logger.info(f"RuntimeError: {re}")
                break
            except Exception as e:
                close_reason = "error"
                logger.error(f"Error processing WebSocket message: {data}")
                logger.exception(e)
    finally:
        for subscription_id, (
            symbol,
            service_subscription_id,
        ) in subscriptions.items():
            try:
                await service.unsubscribe_realtime(
                    service_subscription_id,
                    _send_events=not _is_persister_subscription(subscription_id),
                )
                logger.info(f"Cleaned up subscription {subscription_id}")
            except Exception as e:
                logger.exception(e)
        metrics.ws_connection_closed(WS_ENDPOINT, close_reason)


def _is_persister_subscription(subscription_id: str) -> bool:
    return subscription_id.startswith("persister_")
