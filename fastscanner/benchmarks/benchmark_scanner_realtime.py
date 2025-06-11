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
from fastscanner.pkg.logging import load_logging_config
from fastscanner.services.indicators.clock import ClockRegistry, LocalClock
from fastscanner.services.registry import ApplicationRegistry
from fastscanner.services.scanners.lib.gap import ATRGapDownScanner
from fastscanner.services.scanners.service import ScannerService, SubscriptionHandler

load_logging_config()
logger = logging.getLogger(__name__)

SYMBOLS_FILE = "data/symbols/polygon_symbols.json"
STREAM_PREFIX = "candles_min_"
NO_DATA_TIMEOUT = 10

batch_start_time = None
last_received_time = None
total_messages = 0

ClockRegistry.set(LocalClock())


class BenchmarkScannerHandler(SubscriptionHandler):
    def handle(self, symbol: str, new_row: pd.Series, passed: bool) -> pd.Series:
        global batch_start_time, last_received_time, total_messages

        now = time.time()
        ts = new_row.name

        log_ts = datetime.now().strftime("%H:%M:%S")
        candle_ts = ts.strftime("%H:%M:%S")  # type: ignore
        if passed:
            logger.info(
                f"[{symbol}] LogTime: {log_ts} | CandleTime: {candle_ts} | Passed: {passed} | Data: {new_row.to_dict()}"
            )
        if batch_start_time is None:
            batch_start_time = now
        last_received_time = now
        total_messages += 1
        return new_row


def get_symbols_from_file() -> list[str]:
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

    service = ScannerService(candles=candles, channel=redis_channel)

    symbols = get_symbols_from_file()
    symbols = symbols[:1000]

    scanner = ATRGapDownScanner(
        min_adv=1_000_000,
        min_adr=1.0,
        atr_multiplier=1.5,
        min_volume=500_000,
        start_time=datetime.strptime("09:30", "%H:%M").time(),
        end_time=datetime.strptime("16:00", "%H:%M").time(),
    )

    tasks = []
    for symbol in symbols:
        tasks.append(
            asyncio.create_task(
                service.subscribe_realtime(
                    symbol=symbol,
                    freq="3min",
                    scanner=scanner,
                    handler=BenchmarkScannerHandler(),
                )
            )
        )

    await asyncio.gather(*tasks, monitor_batch_timeout())


if __name__ == "__main__":
    asyncio.run(main())
