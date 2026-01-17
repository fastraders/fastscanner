import argparse
import asyncio
import logging
import math
from datetime import date

from fastscanner.adapters.candle.polygon import PolygonCandlesProvider
from fastscanner.adapters.fundamental.eodhd import EODHDFundamentalStore
from fastscanner.pkg import config
from fastscanner.pkg.clock import ClockRegistry, LocalClock
from fastscanner.pkg.logging import load_logging_config
from fastscanner.services.exceptions import NotFound

load_logging_config()
logger = logging.getLogger(__name__)


async def run_data_collect(only_active: bool = False) -> None:
    eodhd = EODHDFundamentalStore(
        config.EOD_HD_BASE_URL,
        config.EOD_HD_API_KEY,
        max_concurrent_requests=10,
        max_requests_per_min=800,
    )
    polygon = PolygonCandlesProvider(config.POLYGON_BASE_URL, config.POLYGON_API_KEY)
    ClockRegistry.set(LocalClock())
    today = ClockRegistry.clock.today()
    if only_active:
        all_symbols = await polygon.active_symbols()
    else:
        all_symbols = await polygon.all_symbols()
    total_symbols = len(all_symbols)

    ref_day = date(2026, 1, 1)
    batch_size = 2000
    n_batches = math.ceil(len(all_symbols) / batch_size)
    batch_offset = (today - ref_day).days % n_batches
    all_symbols = all_symbols[
        batch_offset * batch_size : (batch_offset + 1) * batch_size
    ]

    logger.info(
        f"Collecting fundamental data for {'active' if only_active else 'all'} symbols, batch offset {batch_offset} / {n_batches}, total symbols {total_symbols}"
    )

    tasks = [asyncio.create_task(eodhd.reload(symbol)) for symbol in all_symbols]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    for r in results:
        if isinstance(r, NotFound):
            logger.warning(r)

    logger.info("Finished fundamental data collection")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run daily data collection")
    parser.add_argument(
        "--only-active",
        action="store_true",
        help="Collect data only for active symbols instead of all symbols",
        default=False,
    )
    args = parser.parse_args()

    asyncio.run(run_data_collect(only_active=args.only_active))


if __name__ == "__main__":
    main()
