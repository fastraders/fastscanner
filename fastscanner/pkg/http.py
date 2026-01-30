import asyncio
import logging
import time
import traceback

import httpx

logger = logging.getLogger(__name__)

RETRIABLE_STATUS = [429, 500, 502, 503, 504]


class MaxRetryError(Exception):
    pass


def retry_request(client: httpx.Client, *args, **kwargs):
    retry_count = 0
    while retry_count <= 3:
        try:
            response = client.request(*args, **kwargs)
            if (
                200 <= response.status_code < 300
            ) or response.status_code not in RETRIABLE_STATUS:
                return response
            logger.warning(
                f"Status code: {response.status_code}\nContent:\n{response.content.decode('utf-8')}"
            )
        except httpx.RequestError:
            logger.warning(f"RequestError: request failed. Will retry...")
        logger.info(f"Retry number {retry_count + 1}...")
        time.sleep(0.1 * 2**retry_count)
        retry_count += 1
    raise MaxRetryError("Max retries reached")


async def async_retry_request(
    client: httpx.AsyncClient, *args, **kwargs
) -> httpx.Response:
    retry_count = 0
    max_retries = 7

    while retry_count <= max_retries:
        try:
            response = await client.request(*args, **kwargs)
            if (
                200 <= response.status_code < 300
                or response.status_code not in RETRIABLE_STATUS
            ):
                return response

            logger.warning(
                f"Status code: {response.status_code}\nContent:\n{response.content.decode('utf-8')}"
            )

        except httpx.RequestError as ex:
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
