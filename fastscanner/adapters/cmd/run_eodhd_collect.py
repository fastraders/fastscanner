import asyncio
import logging

from fastscanner.adapters.candle.polygon import PolygonCandlesProvider
from fastscanner.adapters.fundamental.eodhd import EODHDFundamentalStore
from fastscanner.pkg import config
from fastscanner.pkg.logging import load_logging_config
from fastscanner.services.exceptions import NotFound

load_logging_config()
logger = logging.getLogger(__name__)


async def run_data_collect():
    eodhd = EODHDFundamentalStore(
        config.EOD_HD_BASE_URL,
        config.EOD_HD_API_KEY,
        max_concurrent_requests=10,
        max_requests_per_min=800,
    )
    polygon = PolygonCandlesProvider(config.POLYGON_BASE_URL, config.POLYGON_API_KEY)

    all_symbols = await polygon.all_symbols()
    tasks = [asyncio.create_task(eodhd.get(symbol)) for symbol in all_symbols]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    for r in results:
        if isinstance(r, NotFound):
            logger.warning(r)


if __name__ == "__main__":
    asyncio.run(run_data_collect())
