import asyncio
import logging
from datetime import date, timedelta

from fastscanner.adapters.candle.partitioned_csv import PartitionedCSVCandlesProvider
from fastscanner.adapters.candle.polygon import PolygonCandlesProvider
from fastscanner.pkg import config

logger = logging.getLogger(__name__)


async def collect_daily_data() -> None:
    today = date.today()
    provider = PolygonCandlesProvider(
        base_url=config.POLYGON_BASE_URL, api_key=config.POLYGON_API_KEY
    )
    partitioned_provider = PartitionedCSVCandlesProvider(provider)

    symbols = await provider.all_symbols()

    tasks = [
        partitioned_provider.collect_expired_data(symbol, today) for symbol in symbols
    ]

    results = await asyncio.gather(*tasks)

    for symbol, data_collected in zip(symbols, results):
        if data_collected:
            logger.info(f"Data collection completed for {symbol}")

    logger.info("Data collection completed for all symbols")


def main() -> None:
    asyncio.run(collect_daily_data())


if __name__ == "__main__":
    main()
