import asyncio
import logging
import time

import httpx

from fastscanner.pkg.observability import metrics

logger = logging.getLogger(__name__)

RETRIABLE_STATUS = [429, 500, 502, 503, 504]


class MaxRetryError(Exception):
    pass


def retry_request(client: httpx.Client, *args, **kwargs):
    source, endpoint = _extract_metric_attrs(kwargs)
    retry_count = 0
    while retry_count <= 3:
        try:
            start = time.perf_counter()
            response = client.request(*args, **kwargs)
            elapsed = time.perf_counter() - start
            metrics.external_request(source, endpoint, response.status_code, elapsed)
            if (
                200 <= response.status_code < 300
            ) or response.status_code not in RETRIABLE_STATUS:
                return response
            logger.warning(
                f"Status code: {response.status_code}\nContent:\n{response.content.decode('utf-8')[:200]}"
            )
        except httpx.RequestError as ex:
            metrics.external_request_error(source, endpoint, type(ex).__name__)
            logger.warning(f"RequestError: request failed. Will retry...")
        logger.info(f"Retry number {retry_count + 1}...")
        time.sleep(0.1 * 2**retry_count)
        retry_count += 1
    raise MaxRetryError("Max retries reached")


async def async_retry_request(
    client: httpx.AsyncClient, *args, **kwargs
) -> httpx.Response:
    source, endpoint = _extract_metric_attrs(kwargs)
    retry_count = 0
    max_retries = 7

    while retry_count <= max_retries:
        try:
            start = time.perf_counter()
            response = await client.request(*args, **kwargs)
            elapsed = time.perf_counter() - start
            metrics.external_request(source, endpoint, response.status_code, elapsed)
            if (
                200 <= response.status_code < 300
                or response.status_code not in RETRIABLE_STATUS
            ):
                return response

            logger.warning(
                f"Status code: {response.status_code}\nContent:\n{response.content.decode('utf-8')}"
            )

        except httpx.RequestError as ex:
            metrics.external_request_error(source, endpoint, type(ex).__name__)
            method, url = (
                args[:2]
                if len(args) == 2
                else (kwargs.get("method"), kwargs.get("url"))
            )
            logger.warning(f"Request: {method} {url} RequestError: {ex}. Will retry...")

        retry_count += 1
        logger.info(f"Retry number {retry_count}...")
        await asyncio.sleep(0.1 * 2**retry_count)

    raise MaxRetryError("Max retries reached")


def _extract_metric_attrs(kwargs: dict) -> tuple[str, str]:
    source = kwargs.pop("metric_source", "unknown")
    endpoint = kwargs.pop("metric_endpoint", "unknown")
    return source, endpoint
