import argparse
import asyncio
import logging
import os
from collections import defaultdict
from datetime import date, timedelta
from typing import Any

import pandas as pd

from fastscanner.adapters.candle.partitioned_csv import PartitionedCSVCandlesProvider
from fastscanner.adapters.candle.polygon import PolygonCandlesProvider
from fastscanner.pkg import config
from fastscanner.pkg.clock import LOCAL_TIMEZONE, ClockRegistry, FixedClock, LocalClock
from fastscanner.services.indicators.ports import CandleCol, CandleStore

logging.getLogger().setLevel(logging.INFO)
logger = logging.getLogger(__name__)


class SimpleFormatter(logging.Formatter):
    def format(self, record):
        return record.getMessage()


handler = logging.StreamHandler()
handler.setFormatter(SimpleFormatter())
logger.addHandler(handler)
logger.setLevel(logging.INFO)


class IntegrityIssue:
    def __init__(
        self,
        symbol: str,
        freq: str,
        date: date,
        issue_type: str,
        details: dict[str, Any],
    ):
        self.symbol = symbol
        self.freq = freq
        self.date = date
        self.issue_type = issue_type
        self.details = details

    def __str__(self) -> str:
        return f"[{self.symbol}][{self.freq}] {self.issue_type}: {self.details}"


class CandleIntegrityChecker:
    REALTIME_DIR = os.path.join(config.DATA_BASE_DIR, "data", "realtime")

    def __init__(self, provider: CandleStore):
        self._provider = provider

    async def check_symbol(
        self, symbol: str, check_date: date, freqs: list[str]
    ) -> list[IntegrityIssue]:
        issues: list[IntegrityIssue] = []
        for freq in freqs:
            freq_issues = await self._check_freq(symbol, check_date, freq)
            issues.extend(freq_issues)
        return issues

    async def _check_freq(
        self, symbol: str, check_date: date, freq: str
    ) -> list[IntegrityIssue]:
        realtime_df = self._load_realtime_data(symbol, check_date, freq)
        if isinstance(realtime_df, IntegrityIssue):
            return [realtime_df]

        source_df = await self._load_source_data(symbol, check_date, freq, realtime_df)
        if isinstance(source_df, IntegrityIssue):
            return [source_df]

        issues: list[IntegrityIssue] = []

        issues.extend(
            self._check_duplicates(symbol, check_date, freq, realtime_df, "REALTIME")
        )
        issues.extend(
            self._check_duplicates(symbol, check_date, freq, source_df, "SOURCE")
        )
        issues.extend(
            self._check_missing_and_extra_candles(
                symbol, check_date, freq, realtime_df, source_df
            )
        )
        issues.extend(
            self._check_ohlcv_mismatches(
                symbol, check_date, freq, realtime_df, source_df
            )
        )
        issues.extend(
            self._check_sort_order(symbol, check_date, freq, realtime_df, source_df)
        )

        return issues

    def _load_realtime_data(
        self, symbol: str, check_date: date, freq: str
    ) -> pd.DataFrame | IntegrityIssue:
        realtime_path = os.path.join(
            self.REALTIME_DIR, symbol, freq, f"{check_date.isoformat()}.csv"
        )

        if not os.path.exists(realtime_path):
            return IntegrityIssue(
                symbol,
                freq,
                check_date,
                "MISSING_REALTIME_DATA",
                {"path": realtime_path},
            )

        try:
            realtime_df = pd.read_csv(realtime_path)
            if realtime_df.empty:
                return realtime_df

            realtime_df[CandleCol.DATETIME] = pd.to_datetime(
                realtime_df[CandleCol.DATETIME], utc=True
            )
            return realtime_df.set_index(CandleCol.DATETIME).tz_convert(LOCAL_TIMEZONE)
        except Exception as e:
            return IntegrityIssue(
                symbol,
                freq,
                check_date,
                "ERROR_READING_REALTIME",
                {"error": str(e), "path": realtime_path},
            )

    async def _load_source_data(
        self, symbol: str, check_date: date, freq: str, realtime_df: pd.DataFrame
    ) -> pd.DataFrame | IntegrityIssue:
        try:
            if realtime_df.empty:
                source_df = await self._provider.get(
                    symbol, check_date, check_date, freq, adjusted=False
                )
            else:
                start_time = realtime_df.index.min().date()
                end_time = realtime_df.index.max().date()
                source_df = await self._provider.get(
                    symbol, start_time, end_time, freq, adjusted=False
                )
                start_ts = realtime_df.index.min()
                end_ts = realtime_df.index.max()
                source_df = source_df.loc[start_ts:end_ts]

            if source_df.empty:
                return IntegrityIssue(symbol, freq, check_date, "EMPTY_SOURCE_DATA", {})
            return source_df
        except Exception as e:
            return IntegrityIssue(
                symbol, freq, check_date, "ERROR_READING_SOURCE", {"error": str(e)}
            )

    def _check_duplicates(
        self, symbol: str, check_date: date, freq: str, df: pd.DataFrame, source: str
    ) -> list[IntegrityIssue]:
        duplicated_indices = df.index[df.index.duplicated(keep=False)]
        if len(duplicated_indices) == 0:
            return []

        return [
            IntegrityIssue(
                symbol,
                freq,
                check_date,
                f"DUPLICATE_CANDLES_IN_{source}",
                {
                    "count": len(duplicated_indices),
                    "timestamps": sorted(
                        [str(ts) for ts in duplicated_indices.unique()]
                    ),
                },
            )
        ]

    def _check_missing_and_extra_candles(
        self,
        symbol: str,
        check_date: date,
        freq: str,
        realtime_df: pd.DataFrame,
        source_df: pd.DataFrame,
    ) -> list[IntegrityIssue]:
        realtime_timestamps = set(realtime_df.index)
        source_timestamps = set(source_df.index)

        missing_in_source = realtime_timestamps - source_timestamps
        extra_in_source = source_timestamps - realtime_timestamps

        issues: list[IntegrityIssue] = []

        if missing_in_source:
            issues.append(
                IntegrityIssue(
                    symbol,
                    freq,
                    check_date,
                    "MISSING_CANDLES_IN_SOURCE",
                    {
                        "count": len(missing_in_source),
                        "timestamps": sorted([str(ts) for ts in missing_in_source]),
                    },
                )
            )

        if extra_in_source:
            issues.append(
                IntegrityIssue(
                    symbol,
                    freq,
                    check_date,
                    "EXTRA_CANDLES_IN_SOURCE",
                    {
                        "count": len(extra_in_source),
                        "timestamps": sorted([str(ts) for ts in extra_in_source]),
                    },
                )
            )

        return issues

    def _check_ohlcv_mismatches(
        self,
        symbol: str,
        check_date: date,
        freq: str,
        realtime_df: pd.DataFrame,
        source_df: pd.DataFrame,
    ) -> list[IntegrityIssue]:
        realtime_unique = realtime_df[~realtime_df.index.duplicated(keep="first")]
        source_unique = source_df[~source_df.index.duplicated(keep="first")]

        common_timestamps = set(realtime_unique.index) & set(source_unique.index)

        issues: list[IntegrityIssue] = []
        for ts in sorted(common_timestamps):
            mismatches = self._compare_candles(
                realtime_unique.loc[ts], source_unique.loc[ts]
            )
            if mismatches:
                issues.append(
                    IntegrityIssue(
                        symbol,
                        freq,
                        check_date,
                        "OHLCV_MISMATCH",
                        {"timestamp": str(ts), "mismatches": mismatches},
                    )
                )

        return issues

    def _compare_candles(
        self, rt_candle: pd.Series, source_candle: pd.Series
    ) -> dict[str, Any]:
        ohlcv_fields = [
            CandleCol.OPEN,
            CandleCol.HIGH,
            CandleCol.LOW,
            CandleCol.CLOSE,
            CandleCol.VOLUME,
        ]
        mismatches = {}
        for field in ohlcv_fields:
            rt_val = rt_candle[field]
            source_val = source_candle[field]

            if pd.isna(rt_val) and pd.isna(source_val):
                continue
            if pd.isna(rt_val) or pd.isna(source_val):
                mismatches[field] = {"realtime": rt_val, "source": source_val}
            elif abs(rt_val - source_val) > 1e-6:
                mismatches[field] = {
                    "realtime": float(rt_val),
                    "source": float(source_val),
                }

        return mismatches

    def _check_sort_order(
        self,
        symbol: str,
        check_date: date,
        freq: str,
        realtime_df: pd.DataFrame,
        source_df: pd.DataFrame,
    ) -> list[IntegrityIssue]:
        issues: list[IntegrityIssue] = []

        if not realtime_df.index.is_monotonic_increasing:
            issues.append(
                IntegrityIssue(symbol, freq, check_date, "REALTIME_NOT_SORTED", {})
            )

        if not source_df.index.is_monotonic_increasing:
            issues.append(
                IntegrityIssue(symbol, freq, check_date, "SOURCE_NOT_SORTED", {})
            )

        return issues


def _discover_symbols_with_realtime_data(
    check_date: date, freqs: list[str]
) -> list[str]:
    realtime_dir = os.path.join(config.DATA_BASE_DIR, "data", "realtime")
    if not os.path.exists(realtime_dir):
        return []

    symbols_set: set[str] = set()
    date_str = check_date.isoformat()

    for symbol in os.listdir(realtime_dir):
        symbol_path = os.path.join(realtime_dir, symbol)
        if not os.path.isdir(symbol_path):
            continue

        for freq in freqs:
            freq_path = os.path.join(symbol_path, freq)
            if not os.path.exists(freq_path):
                continue

            date_file = os.path.join(freq_path, f"{date_str}.csv")
            if os.path.exists(date_file):
                symbols_set.add(symbol)
                break

    return sorted(list(symbols_set))


async def check_integrity(
    symbols: list[str] | None = None,
    check_date: date | None = None,
    freqs: list[str] | None = None,
) -> None:
    if check_date is None:
        check_date = ClockRegistry.clock.today() - timedelta(days=1)
    if freqs is None:
        freqs = ["5s", "1min"]
    if symbols is None:
        symbols = _discover_symbols_with_realtime_data(check_date, freqs)
        if not symbols:
            logger.warning(f"No symbols with realtime data found for {check_date}")
            return

    logger.info(
        f"Starting integrity check for {len(symbols)} symbols on {check_date} for freqs: {freqs}"
    )

    polygon = PolygonCandlesProvider(
        config.POLYGON_BASE_URL,
        config.POLYGON_API_KEY,
        max_requests_per_sec=2,
        max_concurrent_requests=2,
    )
    provider = PartitionedCSVCandlesProvider(polygon)
    checker = CandleIntegrityChecker(provider)

    all_issues: list[IntegrityIssue] = []
    issues_by_type: dict[str, int] = defaultdict(int)

    for symbol in symbols:
        try:
            issues = await checker.check_symbol(symbol, check_date, freqs)
            all_issues.extend(issues)
            for issue in issues:
                issues_by_type[issue.issue_type] += 1
        except Exception as e:
            logger.error(f"Error checking symbol {symbol}: {e}")

    _log_summary(check_date, symbols, freqs, all_issues, issues_by_type)


def _log_summary(
    check_date: date,
    symbols: list[str],
    freqs: list[str],
    all_issues: list[IntegrityIssue],
    issues_by_type: dict[str, int],
) -> None:
    symbols_with_issues = {issue.symbol for issue in all_issues}
    logger.info(f"{'='*80}")
    logger.info(f"INTEGRITY CHECK SUMMARY")
    logger.info(f"{'='*80}")
    logger.info(f"Date: {check_date}")
    logger.info(f"Symbols checked: {len(symbols)}")
    logger.info(f"Symbols with issues: {len(symbols_with_issues)}")
    logger.info(f"Frequencies checked: {freqs}")
    logger.info(f"Total issues found: {len(all_issues)}")
    logger.info(f"Issues by type:")
    for issue_type, count in sorted(issues_by_type.items()):
        logger.info(f"  {issue_type}: {count}")

    if all_issues:
        logger.info(f"{'='*80}")
        logger.info(f"DETAILED ISSUES")
        logger.info(f"{'='*80}")
        for issue in all_issues:
            logger.info(issue)


def main() -> None:
    parser = argparse.ArgumentParser(description="Check candle data integrity")
    parser.add_argument(
        "--symbols",
        nargs="+",
        help="Symbols to check (defaults to all symbols with realtime data for the date)",
        default=None,
    )
    parser.add_argument(
        "--date",
        type=str,
        help="Date to check in YYYY-MM-DD format (defaults to yesterday)",
        default=None,
    )
    parser.add_argument(
        "--freqs",
        nargs="+",
        help="Frequencies to check",
        default=["5s", "1min"],
    )

    args = parser.parse_args()

    ClockRegistry.set(FixedClock(LocalClock().now()))

    check_date_val = None
    if args.date:
        check_date_val = date.fromisoformat(args.date)

    asyncio.run(check_integrity(args.symbols, check_date_val, args.freqs))


if __name__ == "__main__":
    main()
