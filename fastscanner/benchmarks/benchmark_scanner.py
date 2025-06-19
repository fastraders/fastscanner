import asyncio
import logging
import math
import time as time_count
from datetime import date, time

import pandas as pd

from fastscanner.adapters.candle.partitioned_csv import PartitionedCSVCandlesProvider
from fastscanner.adapters.candle.polygon import PolygonCandlesProvider
from fastscanner.adapters.fundamental.eodhd import EODHDFundamentalStore
from fastscanner.adapters.holiday.exchange_calendars import (
    ExchangeCalendarsPublicHolidaysStore,
)
from fastscanner.adapters.realtime.void_channel import VoidChannel
from fastscanner.pkg import config
from fastscanner.pkg.logging import load_logging_config
from fastscanner.services.indicators.service import IndicatorsService
from fastscanner.services.registry import ApplicationRegistry
from fastscanner.services.scanners.lib.gap import ATRGapDownScanner, ATRGapUpScanner
from fastscanner.services.scanners.lib.parabolic import (
    ATRParabolicDownScanner,
    ATRParabolicUpScanner,
    DailyATRParabolicDownScanner,
    DailyATRParabolicUpScanner,
)

load_logging_config()
logger = logging.getLogger(__name__)


async def run():
    polygon = PolygonCandlesProvider(
        config.POLYGON_BASE_URL,
        config.POLYGON_API_KEY,
        max_requests_per_sec=50,
    )
    candles = PartitionedCSVCandlesProvider(polygon)
    holidays = ExchangeCalendarsPublicHolidaysStore()
    fundamentals = EODHDFundamentalStore(
        config.EOD_HD_BASE_URL,
        config.EOD_HD_API_KEY,
    )
    indicator_service = IndicatorsService(candles, fundamentals, VoidChannel())

    ApplicationRegistry.init(candles, fundamentals, holidays)
    ApplicationRegistry.set_indicators(indicator_service)

    start_date = date(2023, 1, 1)
    end_date = date(2023, 3, 31)
    freq = "1d"
    symbols = await polygon.all_symbols()
    #symbols = symbols[:1000]
    result: pd.DataFrame | None = None
    scanner = DailyATRParabolicDownScanner(
        min_adv=1_000_000,
        min_adr=0.1,
        atr_multiplier=0.5,
        min_market_cap=1e9,  # only companies > $1B market cap
        max_market_cap=math.inf,  # no upper limit
    )

    logger.info(f"Running scanner for {len(symbols)} symbols")
    start_time = time_count.time()
    for i, symbol in enumerate(symbols, start=1):
        df = await scanner.scan(
            symbol=symbol,
            start=start_date,
            end=end_date,
            freq=freq,
        )
        if i % 100 == 0:
            end_time = time_count.time()
            time_left = (end_time - start_time) * (len(symbols) - i) / i
            logger.info(
                f"Processed {i} symbols. Estimated time left: {time_left:.2f} seconds"
            )

        if df.empty:
            continue

        df.loc[:, "symbol"] = symbol
        if result is None:
            result = df
        else:
            result = pd.concat([result, df])

    logger.info(
        f"Finished processing {len(symbols)} symbols in {time_count.time() - start_time:.4f} seconds"
    )
    if result is None:
        logger.info("No rows passed the given scanner criteria")
        return

    end_time = time_count.time()
    matching_symbols = result["symbol"].unique()
    logger.info(f"Found {len(matching_symbols)} matching symbols")
    logger.info(f"Average rows per symbol: {len(result) / len(matching_symbols):.2f}")
    logger.info(
        f"Processing rate: {len(symbols) / (end_time - start_time):.2f} symbols/second"
    )
    logger.info(result.head(10))


if __name__ == "__main__":
    asyncio.run(run())
