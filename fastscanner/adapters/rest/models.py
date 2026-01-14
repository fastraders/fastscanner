from datetime import date
from typing import Any, Dict, List

from pydantic import BaseModel


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
