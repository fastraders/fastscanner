import asyncio
import logging
import math
import multiprocessing
import os
import time as time_count
from datetime import date, datetime, time, timedelta

import pandas as pd

from fastscanner.adapters.cache.dragonfly import DragonflyCache
from fastscanner.adapters.candle.partitioned_csv import PartitionedCSVCandlesProvider
from fastscanner.adapters.candle.polygon import PolygonCandlesProvider
from fastscanner.adapters.fundamental.eodhd import EODHDFundamentalStore
from fastscanner.adapters.holiday.exchange_calendars import (
    ExchangeCalendarsPublicHolidaysStore,
)
from fastscanner.adapters.realtime.void_channel import VoidChannel
from fastscanner.pkg import config
from fastscanner.pkg.clock import ClockRegistry, FixedClock, LocalClock
from fastscanner.pkg.logging import load_logging_config
from fastscanner.services.indicators.lib import Indicator
from fastscanner.services.indicators.lib.candle import PositionInRangeIndicator
from fastscanner.services.indicators.lib.daily import (
    DailyATRGapIndicator,
    DailyATRIndicator,
    DailyGapIndicator,
)
from fastscanner.services.indicators.lib.fundamental import (
    DaysFromEarningsIndicator,
    DaysToEarningsIndicator,
)
from fastscanner.services.indicators.service import IndicatorsService
from fastscanner.services.registry import ApplicationRegistry
from fastscanner.services.scanners.lib.day2 import Day2GapScanner
from fastscanner.services.scanners.lib.gap import ATRGapDownScanner, ATRGapUpScanner
from fastscanner.services.scanners.lib.parabolic import (
    ATRParabolicDownScanner,
    DailyATRParabolicDownScanner,
    DailyATRParabolicUpScanner,
)
from fastscanner.services.scanners.lib.range_gap import (
    HighRangeGapUpScanner,
    LowRangeGapDownScanner,
)
from fastscanner.services.scanners.lib.smallcap import SmallCapUpScanner
from fastscanner.services.scanners.ports import Scanner

load_logging_config()
logger = logging.getLogger(__name__)

REPORT_DIR = "output/scan_results"


async def _add_report_indicators(
    df: pd.DataFrame, symbol: str, freq: str
) -> pd.DataFrame:
    fundamental_data = await ApplicationRegistry.fundamentals.get(symbol)
    df.loc[:, "symbol"] = symbol
    df.loc[:, "scan_time"] = (df.index + pd.Timedelta(freq)).time  # type: ignore
    df.loc[:, "type"] = fundamental_data.type
    df.loc[:, "exchange"] = fundamental_data.exchange
    df.loc[:, "country"] = fundamental_data.country
    df.loc[:, "city"] = fundamental_data.city
    df.loc[:, "industry"] = fundamental_data.gic_industry
    df.loc[:, "sector"] = fundamental_data.gic_sector
    df.loc[:, "shares_float"] = fundamental_data.shares_float
    df.loc[:, "beta"] = fundamental_data.beta
    df.loc[:, "percent_insiders"] = fundamental_data.insiders_ownership_perc
    df.loc[:, "percent_institutions"] = fundamental_data.institutional_ownership_perc

    indicators: list[Indicator] = [
        DaysFromEarningsIndicator(),
        DaysToEarningsIndicator(),
        DailyGapIndicator(),
        DailyATRGapIndicator(period=14),
        DailyATRIndicator(period=14),
        *[
            PositionInRangeIndicator(n_days=n_days)
            for n_days in [2, 5, 10, 20, 50, 100, 200]
        ],
    ]
    for i in indicators:
        if i.column_name() in df.columns:
            continue
        df = await i.extend(symbol, df)
    df.loc[:, "date"] = df.index.date  # type: ignore

    return df.round(4)


async def _run_async(
    scanner: Scanner,
    symbols: list[str],
    start_date: date,
    end_date: date,
    freq: str,
):
    # We set the clock to a fixed date to ensure that adjustments are done correctly
    ClockRegistry.set(LocalClock())
    polygon = PolygonCandlesProvider(
        config.POLYGON_BASE_URL,
        config.POLYGON_API_KEY,
        max_requests_per_sec=5,
        max_concurrent_requests=5,
    )
    candles = PartitionedCSVCandlesProvider(polygon)
    holidays = ExchangeCalendarsPublicHolidaysStore()
    fundamentals = EODHDFundamentalStore(
        config.EOD_HD_BASE_URL,
        config.EOD_HD_API_KEY,
        max_concurrent_requests=5,
    )
    cache = DragonflyCache(
        config.DRAGONFLY_UNIX_SOCKET,
        password=None,
        db=0,
    )
    indicator_service = IndicatorsService(
        candles,
        fundamentals,
        VoidChannel(),
        cache,
        config.NATS_SYMBOL_SUBSCRIBE_CHANNEL,
        config.NATS_SYMBOL_UNSUBSCRIBE_CHANNEL,
        cache_at_seconds=config.CACHE_AT_SECONDS,
        symbols_slow_indicators_subscribe_channel=config.NATS_SYMBOL_SLOW_INDICATORS_SUBSCRIBE_CHANNEL,
        symbols_slow_indicators_unsubscribe_channel=config.NATS_SYMBOL_SLOW_INDICATORS_UNSUBSCRIBE_CHANNEL,
    )

    ApplicationRegistry.init(candles, fundamentals, holidays, cache)
    ApplicationRegistry.set_indicators(indicator_service)

    result: pd.DataFrame | None = None
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

        df.loc[:, "date"] = df.index.date  # type: ignore
        idx_name = df.index.name
        df = df.reset_index().groupby("date").first(skipna=False).set_index(idx_name)  # type: ignore
        df = await _add_report_indicators(df, symbol, freq)

        if result is None:
            result = df
        else:
            result = pd.concat([result, df])

    if result is None:
        return

    return result


def _run_worker(
    args: tuple[Scanner, list[str], date, date, str],
) -> pd.DataFrame | None:
    return asyncio.run(_run_async(*args))


def _next_scan_path(name: str, start_date: date, end_date: date, freq: str):
    base_dir = os.path.join("output", "scanner", name, freq)
    try_num = 0

    def next_path():
        return os.path.join(base_dir, f"{start_date}-{end_date}_{try_num:03d}.csv")

    path = next_path()
    while os.path.exists(path):
        path = next_path()
        try_num += 1
    return path


async def run_scanner():
    polygon = PolygonCandlesProvider(config.POLYGON_BASE_URL, config.POLYGON_API_KEY)

    all_symbols = await polygon.all_symbols()
    # all_symbols = ["ROLR"] # fmt: skip
    start_date = date(2026, 1, 12)
    end_date = date(2026, 1, 16)
    freq = "1min"
    # scanner = ATRGapUpScanner(
    #     min_adv=500_000,
    #     min_adr=0.005,
    #     min_volume=5_000,
    #     atr_multiplier=2,
    #     start_time=time(9, 20),
    #     end_time=time(9, 25),
    #     min_market_cap=500_000_000,
    #     min_days_from_earnings=0,
    #     max_days_from_earnings=0,
    #     days_of_week=[1, 3, 4],  # 0=Monday ... 6=Sunday
    # )
    # Min_avd					500_000
    # Min_adr					0.005
    # Start_time					9:20:00
    # End_time					9:29:00
    # Min_volume					5_000
    # atr_gap					>=2
    # Min_marketcap				500_000_000

    # Days_of_the_week				mon,tue,thu
    scanner = ATRGapDownScanner(
        min_adv=500_000,
        min_adr=0.005,
        min_volume=5_000,
        min_atr_multiplier=2,
        start_time=time(9, 20),
        end_time=time(9, 29),
        min_market_cap=500_000_000,
        days_of_week=[0, 1, 3],  # 0=Monday ... 6=Sunday
    )
    # scanner = ATRParabolicDownScanner(
    #     min_adv=2_000_000,
    #     min_adr=0.005,
    #     atr_multiplier=0.5,
    #     min_volume=50_000,
    #     start_time=time(9, 30),
    #     end_time=time(15, 59),
    #     include_null_market_cap=True,
    # )
    # scanner = DailyATRParabolicDownScanner(
    #     min_adv=2_000_000,
    #     min_adr=0.005,
    #     atr_multiplier=0.5,
    #     include_null_market_cap=True,
    # )
    # scanner = LowRangeGapDownScanner(
    #     min_adv=500_000,
    #     min_adr=0.03,
    #     max_adr=0.07,
    #     start_time=time(9, 20),
    #     end_time=time(9, 29),
    #     min_volume=5_000,
    #     n_days=20,
    #     min_market_cap=500_000_000,
    #     min_atr_gap=-2,
    #     min_days_from_earnings=50,
    #     max_days_from_earnings=100,
    #     # include_null_market_cap=True,
    # )
    # scanner = HighRangeGapUpScanner(
    #     min_adv=500_000,
    #     min_adr=0.005,
    #     start_time=time(9, 20),
    #     end_time=time(9, 29),
    #     min_volume=5_000,
    #     n_days=20,
    #     min_market_cap=500_000_000,
    #     min_days_from_earnings=50,
    #     max_days_from_earnings=100,
    #     days_of_week=[1, 3, 4],  # 0=Monday ... 6=Sunday
    #     # include_null_market_cap=True,
    # )
    scanner = SmallCapUpScanner(
        min_volume=10_000,
        min_gap=0.20,
        shift_periods=[2],
        shift_min_changes=[0.12],
        min_price=0.3,
        # min_market_cap=100_000,
        # max_market_cap=100_000_000,
        include_null_market_cap=True,
        start_time=time(16, 00),
        end_time=time(17, 30),
    )
    # scanner = Day2GapScanner(
    #     min_adv=0,
    #     min_adr=0,
    #     min_gap=0.05,
    #     min_retrace=0.1,
    #     min_price=0.1,
    #     min_market_cap=100_000,
    #     max_market_cap=1_000_000_000,
    #     include_null_market_cap=True,
    # )

    n_workers = 2 * multiprocessing.cpu_count() + 1
    batch_size = math.ceil(len(all_symbols) / n_workers)
    batches = [
        (scanner, all_symbols[i : i + batch_size], start_date, end_date, freq)
        for i in range(0, len(all_symbols), batch_size)
    ]

    path = _next_scan_path(scanner.__class__.__name__, start_date, end_date, freq)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with multiprocessing.Pool(n_workers) as pool:
        results = pool.map(_run_worker, batches)
        dfs = [r for r in results if r is not None and not r.empty]
        if len(dfs) == 0:
            logger.warning("The scanner returned zero results.")
            return
        df = pd.concat(dfs)
    df.to_csv(path)


if __name__ == "__main__":
    asyncio.run(run_scanner())
