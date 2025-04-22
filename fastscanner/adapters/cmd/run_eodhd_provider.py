import logging
import time
from pathlib import Path

from fastscanner.adapters.fundamental.eodhd import EODHDFundamentalStore
from fastscanner.pkg.logging import load_logging_config

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

load_logging_config()
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    fetcher = EODHDFundamentalStore()
    symbol = "AAPL"

    logger.info(f"Fetching fundamental data for {symbol}")

    start = time.time()
    try:
        data = fetcher.get(symbol)
        logger.info("Successfully retrieved fundamental data.")
        print(data)
    except Exception as e:
        logger.error(f"Failed to fetch fundamental data: {e}")
        raise

    end = time.time()
    logger.info(f"Total time taken: {end - start:.2f} seconds")
