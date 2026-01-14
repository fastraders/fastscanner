import logging
from datetime import date
from typing import Any

import httpx

from fastscanner.adapters.rest.models import ScanRequest, ScanResponse

logger = logging.getLogger(__name__)


class ScanRequestError(Exception):
    pass


class ScannerClient:
    """WebSocket client for consuming candle data with indicators in real-time."""

    def __init__(self, host: str, port: int):
        self._host = host
        self._port = port

    async def scan_day(self, day: date, type: str, params: dict[str, Any]) -> set[str]:
        """Scan candles for a specific day."""
        url = f"http://{self._host}:{self._port}/api/scanners/{type}/scans"
        async with httpx.AsyncClient(timeout=600) as client:
            request = ScanRequest(
                start=day,
                end=day,
                freq="1d",
                type=type,
                params=params,
            )

            response = await client.post(url, json=request.model_dump(mode="json"))
            if response.status_code != 200:
                raise ScanRequestError(
                    f"Failed to scan day {day} with scanner {type}: "
                    f"{response.status_code} {response.text}"
                )
            response = ScanResponse.model_validate_json(response.content)

            return {result["symbol"] for result in response.results}
