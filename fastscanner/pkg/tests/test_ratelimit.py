import asyncio
import time
from unittest.mock import MagicMock, patch

import pytest

from fastscanner.pkg.ratelimit import RateLimiter


def test_init():
    limiter = RateLimiter(max_requests=10)
    assert limiter._max_requests == 10
    assert limiter._time_window == 1.0
    assert limiter._request_timestamps == []

    limiter = RateLimiter(max_requests=5, time_window=2.0)
    assert limiter._max_requests == 5
    assert limiter._time_window == 2.0
    assert limiter._request_timestamps == []


def test_wait_time_no_requests():
    limiter = RateLimiter(max_requests=5)
    assert limiter._wait_time() == 0.0


def test_wait_time_under_limit():
    with patch("time.time", return_value=100.0):
        limiter = RateLimiter(max_requests=5)
        limiter._request_timestamps = [99.0, 99.5, 100.0]

        assert limiter._wait_time() == 0.0


def test_wait_time_at_limit():
    with patch("time.time", return_value=100.0):
        limiter = RateLimiter(max_requests=5)
        limiter._request_timestamps = [99.1, 99.3, 99.5, 99.7, 99.9]

        assert round(limiter._wait_time(), 1) == 0.1


def test_wait_time_expired_requests():
    with patch("time.time", return_value=100.0):
        limiter = RateLimiter(max_requests=5, time_window=1.0)
        limiter._request_timestamps = [98.5, 98.8, 99.2, 99.5, 99.8]

        wait_time = limiter._wait_time()
        assert limiter._request_timestamps == [99.2, 99.5, 99.8]
        assert wait_time == 0.0


def test_add_request():
    with patch("time.time", return_value=100.0):
        limiter = RateLimiter(max_requests=5)
        limiter._add_request()

        assert limiter._request_timestamps == [100.0]


def test_sync_context_manager_no_wait():
    with patch("time.time", return_value=100.0):
        limiter = RateLimiter(max_requests=5)

        with patch("time.sleep") as mock_sleep:
            with limiter:
                pass

            mock_sleep.assert_not_called()

        assert limiter._request_timestamps == [100.0]


def test_sync_context_manager_with_wait():
    with patch("time.time", return_value=100.0):
        limiter = RateLimiter(max_requests=5)
        limiter._request_timestamps = [99.1, 99.3, 99.5, 99.7, 99.9]

        with patch("time.sleep") as mock_sleep:
            with limiter:
                pass

            mock_sleep.assert_called_once()
            wait_time = mock_sleep.call_args[0][0]
            assert round(wait_time, 1) == 0.1

        assert len(limiter._request_timestamps) == 6
        assert limiter._request_timestamps[-1] == 100.0


@pytest.mark.asyncio
async def test_async_context_manager_no_wait():
    with patch("time.time", return_value=100.0):
        limiter = RateLimiter(max_requests=5)

        mock_sleep = MagicMock()
        original_sleep = asyncio.sleep

        async def mock_async_sleep(seconds):
            mock_sleep(seconds)
            await original_sleep(0)

        with patch("asyncio.sleep", side_effect=mock_async_sleep):
            async with limiter:
                pass

            mock_sleep.assert_not_called()

        assert limiter._request_timestamps == [100.0]


@pytest.mark.asyncio
async def test_async_context_manager_with_wait():
    with patch("time.time", return_value=100.0):
        limiter = RateLimiter(max_requests=5)
        limiter._request_timestamps = [99.1, 99.3, 99.5, 99.7, 99.9]

        mock_sleep = MagicMock()
        original_sleep = asyncio.sleep

        async def mock_async_sleep(seconds):
            mock_sleep(seconds)
            await original_sleep(0)

        with patch("asyncio.sleep", side_effect=mock_async_sleep):
            async with limiter:
                pass

            assert mock_sleep.call_count == 1
            wait_time = mock_sleep.call_args[0][0]
            assert round(wait_time, 1) == 0.1

        assert len(limiter._request_timestamps) == 6
        assert limiter._request_timestamps[-1] == 100.0


@pytest.mark.asyncio
async def test_async_lock():
    limiter = RateLimiter(max_requests=5)

    limiter._wait_time = MagicMock(return_value=0.0)
    original_add_request = limiter._add_request
    limiter._add_request = MagicMock(side_effect=original_add_request)

    async def task():
        async with limiter:
            await asyncio.sleep(0.01)

    await asyncio.gather(*(task() for _ in range(3)))

    assert limiter._wait_time.call_count == 3
    assert limiter._add_request.call_count == 3


@pytest.mark.asyncio
async def test_rate_limiting_integration():
    limiter = RateLimiter(max_requests=3, time_window=0.1)

    start_time = time.time()

    for _ in range(6):
        async with limiter:
            pass

    end_time = time.time()

    assert (
        end_time - start_time >= 0.1
    ), "Rate limiting didn't delay execution as expected"
