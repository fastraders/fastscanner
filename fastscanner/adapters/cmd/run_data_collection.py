import asyncio
import json
import logging
import os
from calendar import monthrange
from datetime import date, datetime, timedelta

import pandas as pd

from fastscanner.adapters.candle.partitioned_csv import PartitionedCSVCandlesProvider
from fastscanner.adapters.candle.polygon import PolygonCandlesProvider
from fastscanner.pkg import config
from fastscanner.pkg.datetime import split_freq
from fastscanner.services.indicators.ports import CandleCol

logger = logging.getLogger(__name__)

CANDLES_DIR = os.path.join(config.DATA_BASE_DIR, "data", "candles")


def get_symbol_expirations_file(symbol: str) -> str:
    return os.path.join(CANDLES_DIR, symbol, "expirations.json")


def load_expirations(symbol: str) -> dict[str, str]:
    expirations_file = get_symbol_expirations_file(symbol)
    if not os.path.exists(expirations_file):
        return {}
    with open(expirations_file, "r") as f:
        return json.load(f)


def save_expirations(symbol: str, expirations: dict[str, str]) -> None:
    expirations_file = get_symbol_expirations_file(symbol)
    os.makedirs(os.path.dirname(expirations_file), exist_ok=True)
    with open(expirations_file, "w") as f:
        json.dump(expirations, f, indent=2)


def get_partition_key(dt: date, unit: str) -> str:
    if unit.lower() in ("min", "t"):
        return dt.strftime("%Y-%m-%d")
    if unit.lower() in ("h",):
        return dt.strftime("%Y-%m")
    if unit.lower() in ("d",):
        return dt.strftime("%Y")
    raise ValueError(f"Invalid unit: {unit}")


def get_partition_keys_in_range(start: date, end: date, unit: str) -> list[str]:
    keys = pd.date_range(start, end, freq="1d")
    if unit.lower() in ("min", "t"):
        return pd.Series(keys.strftime("%Y-%m-%d")).drop_duplicates().tolist()
    if unit.lower() in ("h",):
        return pd.Series(keys.strftime("%Y-%m")).drop_duplicates().tolist()
    if unit.lower() in ("d",):
        return pd.Series(keys.strftime("%Y")).drop_duplicates().tolist()
    raise ValueError(f"Invalid unit: {unit}")


def save_cache(symbol: str, key: str, freq: str, df: pd.DataFrame) -> None:
    partition_path = os.path.join(CANDLES_DIR, symbol, freq, f"{key}.csv")
    partition_dir = os.path.dirname(partition_path)
    os.makedirs(partition_dir, exist_ok=True)

    unit = split_freq(freq)[1]
    if unit.lower() != "d":
        df = df.tz_convert("utc").tz_convert(None).reset_index()
    else:
        df = df.reset_index()
        df[CandleCol.DATETIME] = df[CandleCol.DATETIME].dt.strftime("%Y-%m-%d")

    df.to_csv(partition_path, index=False)


async def collect_data_for_symbol(
    provider: PolygonCandlesProvider,
    symbol: str,
    freq: str,
    partition_key: str,
    end_date: date,
) -> bool:
    """Collect data for a symbol and frequency from partition_key to end_date.
    Returns True if any data was collected."""

    _, unit = split_freq(freq)

    # Adjust date parsing based on the unit
    if unit in ("min", "t"):
        partition_key_dt = datetime.strptime(partition_key, "%Y-%m-%d").date()
    elif unit == "h":
        partition_key_dt = datetime.strptime(partition_key, "%Y-%m").date()
    elif unit == "d":
        partition_key_dt = date(int(partition_key), 1, 1)
    else:
        raise ValueError(f"Invalid unit: {unit}")

    if partition_key_dt >= end_date:
        return False

    try:
        df = await provider.get(
            symbol=symbol, start=partition_key_dt, end=end_date, freq=freq
        )

        if df.empty:
            return False

        partitioned_provider = PartitionedCSVCandlesProvider(provider)
        partitioned_provider._save_cache(symbol, partition_key, freq, df)

        return True
    except Exception as e:
        logger.error(f"Error collecting data for {symbol} {freq} {partition_key}: {e}")
        return False


async def collect_daily_data() -> None:
    """Collect previous day's data for all symbols and frequencies."""
    today = date.today()
    yesterday = today - timedelta(days=1)

    provider = PolygonCandlesProvider(
        base_url=config.POLYGON_BASE_URL, api_key=config.POLYGON_API_KEY
    )

    partitioned_provider = PartitionedCSVCandlesProvider(provider)

    symbols = await provider.all_symbols()
    symbols = ["MSFT"]
    unit_freqs = {
        "min": ["1min", "2min", "3min", "5min", "10min", "15min"],
        "h": ["1h"],
        "d": ["1d"],
    }

    for symbol in symbols:
        expirations = load_expirations(symbol)

        for unit, freqs in unit_freqs.items():
            tasks = []

            for freq in freqs:
                partition_keys = partitioned_provider._partition_keys_in_range(
                    yesterday, today, unit
                )

                for partition_key in partition_keys:
                    exp_key = f"{partition_key}_{unit}"

                    if exp_key in expirations:
                        exp_date = datetime.strptime(
                            expirations[exp_key], "%Y-%m-%d"
                        ).date()
                        if exp_date >= today:
                            continue

                    task = collect_data_for_symbol(
                        provider, symbol, freq, partition_key, today
                    )
                    tasks.append(task)

            results = await asyncio.gather(*tasks)
            data_collected = any(results)

            if data_collected:
                for partition_key in partition_keys:
                    exp_key = f"{partition_key}_{unit}"
                    expirations[exp_key] = today.strftime("%Y-%m-%d")
                save_expirations(symbol, expirations)
                logger.info(f"Data collection completed for {symbol} {unit} unit")

    logger.info("Data collection completed for all symbols")


def main() -> None:
    asyncio.run(collect_daily_data())


if __name__ == "__main__":
    main()
