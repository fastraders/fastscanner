import asyncio
import logging
from datetime import date, timedelta

from fastscanner.adapters.candle.partitioned_csv import PartitionedCSVCandlesProvider
from fastscanner.adapters.candle.polygon import PolygonCandlesProvider
from fastscanner.pkg import config
from fastscanner.services.indicators.clock import ClockRegistry, LocalClock

logger = logging.getLogger(__name__)


async def collect_daily_data() -> None:
    ClockRegistry.set(LocalClock())
    provider = PolygonCandlesProvider(
        base_url=config.POLYGON_BASE_URL,
        api_key=config.POLYGON_API_KEY,
        max_requests_per_sec=20,
        max_concurrent_requests=20,
    )
    partitioned_provider = PartitionedCSVCandlesProvider(provider)

    symbols = await provider.all_symbols()
    symbols = ["GOOG"]
    tasks = [partitioned_provider.collect_expired_data(symbol) for symbol in symbols]

    await asyncio.gather(*tasks)
    logger.info("Data collection completed for all symbols")


def main() -> None:
    asyncio.run(collect_daily_data())


if __name__ == "__main__":
    main()
