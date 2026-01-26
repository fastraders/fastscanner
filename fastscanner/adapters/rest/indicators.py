import json
import logging
import os
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
from fastscanner.services.exceptions import UnsubscribeSignal
from fastscanner.services.indicators.service import IndicatorParams as Params
from fastscanner.services.indicators.service import (
    IndicatorsService,
    SubscriptionHandler,
)

from .models import ActionType, StatusType
from .services import get_indicators_service, get_indicators_service_ws

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
    def __init__(self, websocket: WebSocket, subscription_id: str):
        self._websocket = websocket
        self._subscription_id = subscription_id

    async def handle(self, symbol: str, new_row: pd.Series) -> pd.Series:
        candle = new_row.to_dict()
        message = IndicatorMessage(
            subscription_id=self._subscription_id,
            symbol=symbol,
            timestamp=new_row.name,  # type: ignore
            candle=candle,
        )

        await self._send_message(message)
        return new_row

    async def _send_message(self, message: IndicatorMessage):
        try:
            message_json = message.model_dump_json()
            await self._websocket.send_text(message_json)
        except WebSocketDisconnect:
            logger.info("WebSocket disconnected")
            raise UnsubscribeSignal("WebSocket disconnected")
        except RuntimeError as re:
            logger.info(f"RuntimeError: {re}")
            raise UnsubscribeSignal(f"RuntimeError: {re}")
        except Exception as e:
            logger.exception(e)


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
    handler = WebSocketIndicatorHandler(websocket, request.subscription_id)

    indicators_params = [Params(i.type, i.params) for i in request.indicators]

    try:
        subscription_id = await service.subscribe_realtime(
            symbol=request.symbol,
            freq=request.freq,
            indicators=indicators_params,
            handler=handler,
            _send_events=not _is_persister_subscription(request.subscription_id),
        )
    except Exception as e:
        logger.exception(e)
        return SubscriptionResponse(
            status=StatusType.ERROR,
            subscription_id=request.subscription_id,
            message=f"Failed to subscribe to {request.symbol}: {e}",
        )

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
    subscriptions: dict[str, tuple[str, str]] = {}

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
                    response = SubscriptionResponse(
                        status=StatusType.ERROR,
                        subscription_id="",
                        message=f"Unknown action: {action}",
                    )
                    await websocket.send_text(response.model_dump_json())

            except WebSocketDisconnect:
                logger.info("WebSocket disconnected")
                break
            except RuntimeError as re:
                logger.info(f"RuntimeError: {re}")
                break
            except Exception as e:
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


def _is_persister_subscription(subscription_id: str) -> bool:
    return subscription_id.startswith("persister_")
