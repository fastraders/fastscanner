import asyncio
import json
import logging
import os
import time
from datetime import datetime

import pandas as pd

from fastscanner.adapters.candle.partitioned_csv import PartitionedCSVCandlesProvider
from fastscanner.adapters.candle.polygon import PolygonCandlesProvider
from fastscanner.adapters.fundamental.eodhd import EODHDFundamentalStore
from fastscanner.adapters.holiday.exchange_calendars import (
    ExchangeCalendarsPublicHolidaysStore,
)
from fastscanner.adapters.realtime.redis_channel import RedisChannel
from fastscanner.pkg import config
from fastscanner.services.indicators.lib import IndicatorsLibrary
from fastscanner.services.indicators.lib.candle import (
    ATRIndicator,
    CumulativeDailyVolumeIndicator,
    PositionInRangeIndicator,
    PremarketCumulativeIndicator,
)
from fastscanner.services.indicators.lib.daily import (
    DailyATRGapIndicator,
    DailyATRIndicator,
    DailyGapIndicator,
    PrevDayIndicator,
)
from fastscanner.services.indicators.ports import CandleCol
from fastscanner.services.indicators.registry import ApplicationRegistry
from fastscanner.services.indicators.service import (
    IndicatorParams,
    IndicatorsService,
    SubscriptionHandler,
)

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

SYMBOLS_FILE = "data/symbols/symbols.json"
STREAM_PREFIX = "candles_min_"
NO_DATA_TIMEOUT = 10

batch_start_time = None
last_received_time = None
total_messages = 0


class BenchmarkHandler(SubscriptionHandler):
    def handle(self, symbol: str, new_row: pd.Series) -> None:
        global batch_start_time, last_received_time, total_messages
        now = time.time()

        if batch_start_time is None:
            batch_start_time = now
        last_received_time = now
        total_messages += 1
        logger.debug(f"[{symbol}] New row: {new_row.to_dict()}")


async def get_symbols_from_file() -> list[str]:
    if os.path.exists(SYMBOLS_FILE):
        with open(SYMBOLS_FILE, "r") as f:
            symbols = json.load(f)
            logger.info(f"Loaded {len(symbols)} symbols from file.")
            return symbols
    return []


async def monitor_batch_timeout():
    global batch_start_time, last_received_time, total_messages

    while True:
        await asyncio.sleep(1)
        now = time.time()

        if (
            batch_start_time
            and last_received_time
            and (now - last_received_time) > NO_DATA_TIMEOUT
        ):
            logger.info("\n--- Benchmark Report ---")
            logger.info(
                f"Batch started at: {datetime.fromtimestamp(batch_start_time).strftime('%M:%S.%f')}"
            )
            logger.info(
                f"Batch ended at:   {datetime.fromtimestamp(now).strftime('%M:%S.%f')}"
            )
            logger.info(f"Total messages read: {total_messages}")
            logger.info(f"Batch duration: {now - batch_start_time:.6f} seconds\n")
            batch_start_time = None
            last_received_time = None
            total_messages = 0


async def main():
    redis_channel = RedisChannel(
        host=config.REDIS_DB_HOST,
        port=config.REDIS_DB_PORT,
        unix_socket_path=config.UNIX_SOCKET_PATH,
        password=None,
        db=0,
    )
    polygon = PolygonCandlesProvider(config.POLYGON_BASE_URL, config.POLYGON_API_KEY)
    holidays = ExchangeCalendarsPublicHolidaysStore()
    candles = PartitionedCSVCandlesProvider(polygon)
    fundamentals = EODHDFundamentalStore(config.EOD_HD_BASE_URL, config.EOD_HD_API_KEY)

    ApplicationRegistry.init(candles, fundamentals, holidays)

    service = IndicatorsService(
        candles=candles, fundamentals=fundamentals, channel=redis_channel
    )

    symbols = await get_symbols_from_file()

    indicators = [
        IndicatorParams(
            type_=PrevDayIndicator.type(), params={"candle_col": CandleCol.CLOSE}
        ),
        IndicatorParams(type_=DailyGapIndicator.type(), params={}),
        IndicatorParams(type_=DailyATRIndicator.type(), params={"period": 14}),
        IndicatorParams(type_=DailyATRGapIndicator.type(), params={"period": 14}),
        IndicatorParams(
            type_=ATRIndicator.type(),
            params={"period": 14, "freq": "1min"},
        ),
        IndicatorParams(type_=CumulativeDailyVolumeIndicator.type(), params={}),
        IndicatorParams(
            type_=PremarketCumulativeIndicator.type(),
            params={"candle_col": CandleCol.CLOSE, "op": "sum"},
        ),
        IndicatorParams(type_=PositionInRangeIndicator.type(), params={"n_days": 5}),
    ]

    for symbol in symbols:
        try:
            await service.subscribe_realtime(
                symbol=symbol,
                freq="1min",
                indicators=indicators,
                handler=BenchmarkHandler(),
            )
            logger.info(f"Subscribed to: {symbol}")
        except Exception as e:
            logger.warning(f"Subscription failed for {symbol}: {e}")

    await monitor_batch_timeout()


if __name__ == "__main__":
    asyncio.run(main())
