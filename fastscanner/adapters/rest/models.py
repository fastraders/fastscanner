from datetime import date
from enum import Enum
from typing import Any, Dict, List

from pydantic import BaseModel


class ActionType(str, Enum):
    SUBSCRIBE = "subscribe"
    UNSUBSCRIBE = "unsubscribe"


class StatusType(str, Enum):
    SUCCESS = "success"
    ERROR = "error"


class ScanRealtimeSubscribeRequest(BaseModel):
    scanner_id: str
    type: str
    params: Dict[str, Any]
    freq: str
    action = ActionType.SUBSCRIBE


class ScanRealtimeUnsubscribeRequest(BaseModel):
    subscription_id: str
    action = ActionType.UNSUBSCRIBE


class ScanRealtimeSubscribeResponse(BaseModel):
    scanner_id: str


class ScanRequest(BaseModel):
    start: date
    end: date
    freq: str
    type: str
    params: Dict[str, Any]


class ScanResponse(BaseModel):
    results: List[Dict[str, Any]]
    scanner_type: str


class ScannerMessage(BaseModel):
    symbol: str
    scan_time: str
    scanner_id: str
    candle: Dict[str, Any]
