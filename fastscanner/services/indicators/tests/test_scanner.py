from datetime import date, datetime, time
from typing import Any

import pandas as pd
import pytest

from fastscanner.services.indicators.ports import CandleCol, CandleStore, ChannelHandler
from fastscanner.services.indicators.service import IndicatorsService
from fastscanner.services.indicators.tests.fixtures import (
    CandleStoreTest,
    MockFundamentalDataStore,
    MockPublicHolidaysStore,
)
from fastscanner.services.registry import ApplicationRegistry
from fastscanner.services.scanners.lib.gap import ATRGapDownScanner, ATRGapUpScanner
from fastscanner.services.scanners.ports import Scanner


class MockChannel:
    async def subscribe(self, channel_id: str, handler: ChannelHandler) -> None:
        pass

    async def push(
        self, channel_id: str, data: dict[Any, Any], flush: bool = True
    ) -> None:
        pass

    async def flush(self) -> None:
        pass


@pytest.fixture
def candles():
    candle_store = CandleStoreTest()
    fundamental_store = MockFundamentalDataStore()
    holiday_store = MockPublicHolidaysStore()

    ApplicationRegistry.init(
        candles=candle_store, fundamentals=fundamental_store, holidays=holiday_store
    )

    indicators_service = IndicatorsService(
        candles=candle_store, fundamentals=fundamental_store, channel=MockChannel()
    )
    ApplicationRegistry.set_indicators(indicators_service)

    yield candle_store
    ApplicationRegistry.reset()


@pytest.mark.asyncio
async def test_scanner_consistency_between_scan_and_scan_realtime(candles):
    symbol = "AAPL"
    freq = "1m"

    daily_dates = pd.date_range(start=date(2023, 1, 1), end=date(2023, 1, 30))
    daily_data = pd.DataFrame(
        {
            CandleCol.OPEN: [100 + i for i in range(30)],
            CandleCol.HIGH: [105 + i for i in range(30)],
            CandleCol.LOW: [95 + i for i in range(30)],
            CandleCol.CLOSE: [102 + i for i in range(30)],
            CandleCol.VOLUME: [1_000_000 + i * 100_000 for i in range(30)],
        },
        index=daily_dates,
    )

    test_date = date(2023, 1, 31)
    intraday_timestamps = pd.date_range(
        start=datetime(2023, 1, 31, 9, 30),
        end=datetime(2023, 1, 31, 10, 0),
        freq="1min",
    )

    intraday_data = pd.DataFrame(
        {
            CandleCol.OPEN: [140 + i for i in range(len(intraday_timestamps))],
            CandleCol.HIGH: [145 + i for i in range(len(intraday_timestamps))],
            CandleCol.LOW: [135 + i for i in range(len(intraday_timestamps))],
            CandleCol.CLOSE: [142 + i for i in range(len(intraday_timestamps))],
            CandleCol.VOLUME: [
                100_000 + i * 5_000 for i in range(len(intraday_timestamps))
            ],
        },
        index=intraday_timestamps,
    )

    complete_data = pd.concat([daily_data, intraday_data])
    candles.set_data(symbol, complete_data)

    scanners: list[Scanner] = [
        ATRGapDownScanner(
            min_adv=500_000,
            min_adr=0.05,
            atr_multiplier=-1.0,
            min_volume=0,
            start_time=time(9, 20),
            end_time=time(12, 0),
            min_market_cap=0,
        ),
        ATRGapUpScanner(
            min_adv=500_000,
            min_adr=0.05,
            atr_multiplier=1.0,
            min_volume=0,
            start_time=time(9, 20),
            end_time=time(12, 0),
            min_market_cap=0,
        ),
    ]

    for scanner in scanners:
        scanner_name = scanner.__class__.__name__

        scan_result = await scanner.scan(
            symbol=symbol, start=test_date, end=test_date, freq=freq
        )
        scan_passed_timestamps = (
            set(scan_result.index) if not scan_result.empty else set()
        )

        realtime_passed_timestamps = set()
        for timestamp, row in intraday_data.iterrows():
            _, passes_filter = await scanner.scan_realtime(
                symbol=symbol, new_row=row, freq=freq
            )
            if passes_filter:
                realtime_passed_timestamps.add(timestamp)

        assert realtime_passed_timestamps == scan_passed_timestamps, (
            f"{scanner_name} inconsistency:\n"
            f"scan() results: {sorted(scan_passed_timestamps)}\n"
            f"scan_realtime() True results: {sorted(realtime_passed_timestamps)}\n"
            f"Only in scan(): {scan_passed_timestamps - realtime_passed_timestamps}\n"
            f"Only in scan_realtime(): {realtime_passed_timestamps - scan_passed_timestamps}"
        )

    print(f"All {len(scanners)} scanners passed consistency test")
    print(f"Tested {len(intraday_data)} timestamps per scanner")
