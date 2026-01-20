import asyncio
import json
import logging
import os
import time
from datetime import datetime

import pandas as pd
import uvloop

from fastscanner.adapters.candle.partitioned_csv import PartitionedCSVCandlesProvider
from fastscanner.adapters.candle.polygon import PolygonCandlesProvider
from fastscanner.adapters.fundamental.eodhd import EODHDFundamentalStore
from fastscanner.adapters.holiday.exchange_calendars import (
    ExchangeCalendarsPublicHolidaysStore,
)
from fastscanner.adapters.realtime.redis_channel import RedisChannel
from fastscanner.pkg import config
from fastscanner.pkg.clock import ClockRegistry, LocalClock
from fastscanner.pkg.logging import load_logging_config
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
from fastscanner.services.indicators.lib.fundamental import MarketCapIndicator
from fastscanner.services.indicators.ports import CandleCol
from fastscanner.services.indicators.service import (
    IndicatorParams,
    IndicatorsService,
    SubscriptionHandler,
)
from fastscanner.services.registry import ApplicationRegistry

load_logging_config()
logger = logging.getLogger(__name__)

SYMBOLS_FILE = "data/symbols/polygon_symbols.json"
STREAM_PREFIX = "candles.min."
NO_DATA_TIMEOUT = 10

batch_start_time = None
last_received_time = None
total_messages = 0

ClockRegistry.set(LocalClock())


class BenchmarkHandler(SubscriptionHandler):
    def handle(self, symbol: str, new_row: pd.Series) -> None:
        global batch_start_time, last_received_time, total_messages

        now = time.time()
        ts = new_row.name

        log_ts = datetime.now().strftime("%H:%M:%S")
        candle_ts = ts.strftime("%H:%M:%S")  # type: ignore
        logger.info(
            f"[{symbol}] LogTime: {log_ts} | CandleTime: {candle_ts} | Data: {new_row.to_dict()}"
        )
        if batch_start_time is None:
            batch_start_time = now
        last_received_time = now
        total_messages += 1


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
            batch_duration = last_received_time - batch_start_time
            logger.info(f"\nBatch Summary:")
            logger.info(
                f"First message timestamp: {datetime.fromtimestamp(batch_start_time).strftime('%H:%M:%S.%f')[:-3]}"
            )
            logger.info(
                f"Last message timestamp:  {datetime.fromtimestamp(last_received_time).strftime('%H:%M:%S.%f')[:-3]}"
            )
            logger.info(f"Total messages read: {total_messages}")
            logger.info(f"Batch duration: {batch_duration:.6f} seconds\n")
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
        candles=candles,
        fundamentals=fundamentals,
        channel=redis_channel,
        symbols_subscribe_channel=config.NATS_SYMBOL_SUBSCRIBE_CHANNEL,
        symbols_unsubscribe_channel=config.NATS_SYMBOL_UNSUBSCRIBE_CHANNEL,
    )

    symbols = await polygon.all_symbols()
    symbols = symbols[:100]
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
        IndicatorParams(type_=MarketCapIndicator.type(), params={}),
    ]

    tasks = []
    for symbol in symbols:

        tasks.append(
            asyncio.create_task(
                service.subscribe_realtime(
                    symbol=symbol,
                    freq="3min",
                    indicators=indicators,
                    handler=BenchmarkHandler(),
                )
            )
        )

    await asyncio.gather(*tasks, monitor_batch_timeout())


if __name__ == "__main__":
    uvloop.run(main())
