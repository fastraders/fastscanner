import json
import logging
import multiprocessing
import os
import zlib
from calendar import monthrange
from datetime import date, timedelta

import boto3
import botocore.exceptions
import pandas as pd
from mypy_boto3_s3.service_resource import S3ServiceResource

from fastscanner.pkg import config
from fastscanner.pkg.clock import LOCAL_TIMEZONE_STR, ClockRegistry, split_freq
from fastscanner.pkg.http import MaxRetryError, async_retry_request
from fastscanner.pkg.ratelimit import RateLimiter
from fastscanner.services.indicators.ports import CandleCol

logger = logging.getLogger(__name__)


class PolygonCandlesFromTradesCollector:
    def __init__(
        self,
        base_url: str,
        base_trades_dir: str,
        base_candles_dir: str,
        aws_access_key: str,
        aws_secret_key: str,
        max_concurrency: int = 50,
    ):
        self._base_url = base_url
        self._base_trades_dir = base_trades_dir
        self._base_candles_dir = base_candles_dir
        self._aws_access_key = aws_access_key
        self._aws_secret_key = aws_secret_key
        self._max_concurrency = max_concurrency
        self._client: S3ServiceResource | None = None

    @property
    def client(self) -> S3ServiceResource:
        if self._client is None:
            self._client = boto3.resource(
                "s3",
                aws_access_key_id=self._aws_access_key,
                aws_secret_access_key=self._aws_secret_key,
                endpoint_url=self._base_url,
            )
        return self._client

    def _file_path(self, date_: date) -> str:
        return os.path.join(
            self._base_trades_dir, str(date_.year), f"{date_.isoformat()}.csv.gz"
        )

    def _download_file(self, date_: date) -> None:
        object_key = f"us_stocks_sip/trades_v1/{date_.year}/{date_.month:02d}/{date_.isoformat()}.csv.gz"
        local_file_path = self._file_path(date_)
        os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
        self.client.Bucket("flatfiles").download_file(
            object_key,
            local_file_path,
        )

    def _candles_df(self, date_: date, freq: str) -> pd.DataFrame:
        if not os.path.exists(self._file_path(date_)):
            try:
                self._download_file(date_)
            except botocore.exceptions.ClientError as e:
                if (
                    "Error" not in e.response
                    or "Code" not in e.response["Error"]
                    or e.response["Error"]["Code"] != "404"
                ):
                    raise e
                return pd.DataFrame(
                    columns=[
                        "open",
                        "high",
                        "low",
                        "close",
                        "volume",
                    ],
                    index=pd.DatetimeIndex([], name="datetime", tz=LOCAL_TIMEZONE_STR),
                )
        local_file_path = self._file_path(date_)
        uncompressed_path = local_file_path.replace(".gz", "")
        decompressor = zlib.decompressobj(31)
        try:
            with open(local_file_path, "rb") as f_in:
                with open(uncompressed_path, "wb") as f_out:
                    while True:
                        chunk = f_in.read(1024 * 1024)  # Read in 1MB chunks
                        if not chunk:
                            break
                        decompressed_data = decompressor.decompress(chunk)
                        f_out.write(decompressed_data)
                    f_out.write(decompressor.flush())
            df = pd.read_csv(uncompressed_path)
            df["datetime"] = pd.to_datetime(
                df["sip_timestamp"], unit="ns", utc=True
            ).dt.tz_convert(LOCAL_TIMEZONE_STR)
            df = df.sort_values(["datetime", "sequence_number"])
            candles = df.groupby(["ticker", pd.Grouper(key="datetime", freq=freq)]).agg(
                open=pd.NamedAgg(column="price", aggfunc="first"),
                high=pd.NamedAgg(column="price", aggfunc="max"),
                low=pd.NamedAgg(column="price", aggfunc="min"),
                close=pd.NamedAgg(column="price", aggfunc="last"),
                volume=pd.NamedAgg(column="size", aggfunc="sum"),
            )
            # candles = candles.loc[candles["volume"] >= 100]
            return candles
        finally:
            os.remove(uncompressed_path)

    def _save_cache(self, symbol: str, key: str, freq: str, df: pd.DataFrame):
        partition_path = self._partition_path(symbol, key, freq)
        partition_dir = os.path.dirname(partition_path)
        os.makedirs(partition_dir, exist_ok=True)

        unit = split_freq(freq)[1]
        if unit.lower() != "d":
            df = df.tz_convert("utc").tz_convert(None).reset_index()
        else:
            df = df.reset_index()
            df[CandleCol.DATETIME] = df[CandleCol.DATETIME].dt.strftime("%Y-%m-%d")  # type: ignore

        df.to_csv(partition_path, index=False)

    def _partition_path(self, symbol: str, key: str, freq: str) -> str:
        return os.path.join(self._base_candles_dir, symbol, freq, f"{key}.csv")

    def _partition_key(self, dt: date, unit: str) -> str:
        return self._partition_keys(pd.DatetimeIndex([dt]), unit).iat[0]

    def _partition_keys(self, index: pd.DatetimeIndex, unit: str) -> "pd.Series[str]":
        if unit.lower() in ("s", "min", "t"):
            dt = pd.to_timedelta(index.dayofweek, unit="d")
            return pd.Series(
                (index - dt).strftime("%Y-%m-%d"), index=index, name="partition_key"
            )
        if unit.lower() in ("h",):
            return pd.Series(index.strftime("%Y-%m"), index=index, name="partition_key")
        if unit.lower() in ("d",):
            return pd.Series(index.strftime("%Y"), index=index, name="partition_key")
        raise ValueError(f"Invalid unit: {unit}")

    def _partition_keys_in_range(self, start: date, end: date, unit: str) -> list[str]:
        keys = self._partition_keys(pd.date_range(start, end, freq="1d"), unit)
        return keys.drop_duplicates().tolist()

    def _covering_range(self, start: date, end: date, unit: str) -> tuple[date, date]:
        start_key = self._partition_key(start, unit)
        end_key = self._partition_key(end, unit)
        return (
            self._range_from_key(start_key, unit)[0],
            self._range_from_key(end_key, unit)[1],
        )

    def _range_from_key(self, key: str, unit: str) -> tuple[date, date]:
        if unit.lower() in ("s", "min", "t"):
            return date.fromisoformat(key), date.fromisoformat(key) + timedelta(days=6)
        if unit.lower() in ("h",):
            year, month = key.split("-")
            year, month = int(year), int(month)
            _, days = monthrange(year, month)
            return date(year, month, 1), date(year, month, days)
        if unit.lower() in ("d",):
            return date(int(key), 1, 1), date(int(key), 12, 31)
        raise ValueError(f"Invalid unit: {unit}")

    _expirations: dict[str, dict[str, date]]

    def _is_expired(self, symbol: str, key: str, unit: str, today: date) -> bool:
        self._load_expirations(symbol)

        expiration_key = self._expiration_key(key, unit)
        expirations = self._expirations.get(symbol, {})
        if expiration_key not in expirations:
            return False

        return expirations[expiration_key] <= today

    def _mark_expiration(self, symbol: str, key: str, unit: str, today: date) -> None:
        self._load_expirations(symbol)

        _, end = self._range_from_key(key, unit)
        expiration_key = self._expiration_key(key, unit)
        expirations = self._expirations.setdefault(symbol, {})
        if today > end and expiration_key not in expirations:
            return

        if today > end:
            expirations.pop(expiration_key, None)
        else:
            expirations[expiration_key] = today + timedelta(days=1)

        with open(
            os.path.join(self._base_candles_dir, symbol, "expirations.json"), "w"
        ) as f:
            json.dump({key: value.isoformat() for key, value in expirations.items()}, f)

    def _expiration_key(self, key: str, unit: str) -> str:
        return f"{key}_{unit}"

    def _load_expirations(self, symbol):
        if not hasattr(self, "_expirations"):
            self._expirations = {}

        if symbol in self._expirations:
            return

        try:
            with open(
                os.path.join(self._base_candles_dir, symbol, "expirations.json")
            ) as f:
                self._expirations[symbol] = {
                    key: date.fromisoformat(value)
                    for key, value in json.load(f).items()
                }
        except FileNotFoundError:
            self._expirations[symbol] = {}

    def _collect_key(self, key: str, freq: str):
        _, unit = split_freq(freq)
        today = ClockRegistry.clock.today()
        start, end = self._range_from_key(key, unit)

        dfs = [
            self._candles_df(date_.date(), freq) for date_ in pd.date_range(start, end)
        ]
        dfs = [df for df in dfs if not df.empty]

        if len(dfs) == 0:
            return pd.DataFrame(
                columns=[
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                ],
                index=pd.DatetimeIndex([], name="datetime", tz=LOCAL_TIMEZONE_STR),
            )

        df = pd.concat(dfs)
        for symbol, symbol_df in df.groupby(level="ticker"):
            symbol_df = symbol_df.droplevel("ticker")
            self._save_cache(symbol, key, freq, symbol_df)  # type: ignore
            self._mark_expiration(symbol, key, unit, today)  # type: ignore

    def collect(
        self,
        year: int,
        freq: str,
    ):
        if not freq.endswith("s"):
            raise ValueError(
                "Only second-based frequencies are supported to collect from trades."
            )

        _, unit = split_freq(freq)
        today = ClockRegistry.clock.today()
        yday = today - timedelta(days=1)
        start = date(year, 1, 1)
        end = min(date(year, 12, 31), yday)

        with multiprocessing.Pool(processes=self._max_concurrency) as pool:
            pool.starmap(
                self._collect_key,
                [
                    (key, freq)
                    for key in self._partition_keys_in_range(start, end, unit)
                ],
            )
