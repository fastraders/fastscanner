import asyncio
import logging
import time
from contextlib import asynccontextmanager, contextmanager

logger = logging.getLogger(__name__)


class RateLimiter:
    def __init__(self, max_requests: int, time_window: float = 1.0):
        self._max_requests = max_requests
        self._time_window = time_window
        self._request_timestamps = []
        self._lock = asyncio.Lock()

    def _wait_time(self) -> float:
        now = time.time()

        i = 0
        for i in range(len(self._request_timestamps)):
            if self._request_timestamps[i] >= now - self._time_window:
                break

        self._request_timestamps = self._request_timestamps[i:]

        if len(self._request_timestamps) < self._max_requests:
            return 0.0

        return self._request_timestamps[0] + self._time_window - now

    def _add_request(self):
        self._request_timestamps.append(time.time())

    def __enter__(self):
        wait_time = self._wait_time()
        if wait_time > 0:
            time.sleep(wait_time)
        self._add_request()
        return None

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    async def __aenter__(self):
        async with self._lock:
            wait_time = self._wait_time()
            if wait_time > 0:
                await asyncio.sleep(wait_time)
            self._add_request()
        return None

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass
