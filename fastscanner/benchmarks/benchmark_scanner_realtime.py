import asyncio
import json
import logging
import os
import time
from datetime import datetime, time as dt_time

import pandas as pd

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
from fastscanner.services.registry import ApplicationRegistry
from fastscanner.services.scanners.lib.gap import ATRGapDownScanner
from fastscanner.services.scanners.service import ScannerService, SubscriptionHandler
from fastscanner.services.scanners.ports import ScannerParams

load_logging_config()
logger = logging.getLogger(__name__)

SYMBOLS_FILE = "data/symbols/polygon_symbols.json"
STREAM_PREFIX = "candles_min_"
NO_DATA_TIMEOUT = 10

batch_start_time = None
last_received_time = None
total_messages = 0


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


async def monitor_batch_timeout():
    global batch_start_time, last_received_time, total_messages

    while True:
        await asyncio.sleep(1)
        now = time.time()
        if (
            batch_start_time is None
            or last_received_time is None
            or (now - last_received_time) < NO_DATA_TIMEOUT
        ):
            continue

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
    ClockRegistry.set(LocalClock())
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

    service = ScannerService(
        candles=candles, channel=redis_channel, symbols_provider=polygon
    )

    scanner_params = ScannerParams(
        type_="atr_gap_down",
        params={
            "min_adv": 1_000_000.0,
            "min_adr": 1.0,
            "atr_multiplier": 1.5,
            "min_volume": 500_000.0,
            "start_time": dt_time(9, 30),
            "end_time": dt_time(16, 0),
            "min_market_cap": 0.0,
        },
    )

    scanner_id = await service.subscribe_realtime(
        params=scanner_params,
        handler=BenchmarkScannerHandler(),
        freq="1min",
    )

    logger.info(f"Started scanner with ID: {scanner_id}")

    monitor_task = asyncio.create_task(monitor_batch_timeout())

    try:
        await monitor_task
    except KeyboardInterrupt:
        logger.info("Shutting down scanner...")
        await service.unsubscribe_realtime(scanner_id)
        monitor_task.cancel()


if __name__ == "__main__":
    asyncio.run(main())
