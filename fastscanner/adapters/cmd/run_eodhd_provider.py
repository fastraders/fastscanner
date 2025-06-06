import asyncio
import logging
import sys
import time
from pathlib import Path

from fastscanner.adapters.fundamental.eodhd import EODHDFundamentalStore
from fastscanner.pkg import config
from fastscanner.pkg.logging import load_logging_config

load_logging_config()
logger = logging.getLogger(__name__)


async def main():
    fetcher = EODHDFundamentalStore(config.EOD_HD_BASE_URL, config.EOD_HD_API_KEY)
    symbol = "AAPL"

    logger.info(f"Fetching fundamental data for {symbol}")

    start = time.time()
    try:
        data = await fetcher.get(symbol)
        logger.info("Successfully retrieved fundamental data.")
        logger.info(data)
    except Exception as e:
        logger.error(f"Failed to fetch fundamental data: {e}")
        raise

    end = time.time()
    logger.info(f"Total time taken: {end - start:.2f} seconds")


if __name__ == "__main__":
    asyncio.run(main())
