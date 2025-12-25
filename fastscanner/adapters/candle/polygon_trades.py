import asyncio
import json
import logging
import multiprocessing
import os
import zlib
from calendar import monthrange
from datetime import date, timedelta
from time import time as pytime

import boto3
import botocore.config
import botocore.exceptions
import polars as pl
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
        max_concurrency: int = 10,
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
                config=botocore.config.Config(max_pool_connections=100),
            )
        return self._client

    def _file_path(self, date_: date) -> str:
        return os.path.join(
            self._base_trades_dir, str(date_.year), f"{date_.isoformat()}.csv.gz"
        )

    def _download_file(self, date_: date) -> date:
        local_file_path = self._file_path(date_)
        if os.path.exists(local_file_path):
            return date_
        object_key = f"us_stocks_sip/trades_v1/{date_.year}/{date_.month:02d}/{date_.isoformat()}.csv.gz"
        os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
        self.client.Bucket("flatfiles").download_file(
            object_key,
            local_file_path,
        )
        logger.info(f"Downloaded trades file for {date_.isoformat()}")
        return date_

    async def _collect_files(self, start: date, end: date) -> list[date]:
        downloaded_dates: list[date] = []
        tasks: list[asyncio.Task[date]] = []
        curr_date = start
        while curr_date <= end:
            task = asyncio.create_task(
                asyncio.to_thread(self._download_file, curr_date)
            )
            tasks.append(task)
            curr_date += timedelta(days=1)
            if len(tasks) < self._max_concurrency:
                continue
            done, pending = await asyncio.wait(
                tasks, return_when=asyncio.FIRST_COMPLETED
            )
            tasks = list(pending)
            for task in done:
                try:
                    downloaded_dates.append(task.result())
                except botocore.exceptions.ClientError as e:
                    if (
                        "Error" not in e.response
                        or "Code" not in e.response["Error"]
                        or e.response["Error"]["Code"] != "404"
                    ):
                        raise e

        done, _ = await asyncio.wait(tasks)
        for task in done:
            try:
                downloaded_dates.append(task.result())
            except botocore.exceptions.ClientError as e:
                if (
                    "Error" not in e.response
                    or "Code" not in e.response["Error"]
                    or e.response["Error"]["Code"] != "404"
                ):
                    raise e

        return sorted(downloaded_dates)

    async def _collect_latest_files(self) -> list[date]:
        yday = ClockRegistry.clock.today() - timedelta(days=1)
        curr_years = os.listdir(self._base_trades_dir)
        if len(curr_years) > 0:
            last_year = max(curr_years)
        else:
            last_year = str(yday.year)
            os.mkdir(os.path.join(self._base_trades_dir, last_year))
        current_files = [
            fname
            for fname in os.listdir(os.path.join(self._base_trades_dir, last_year))
            if fname.endswith(".csv.gz")
        ]
        if len(current_files) > 0:
            last_date = date.fromisoformat(max(current_files).replace(".csv.gz", ""))
            start = last_date + timedelta(days=1)
        else:
            start = yday
        return await self._collect_files(start, yday)

    def _process_date(self, date_: date, freqs: list[str]) -> None:
        local_file_path = self._file_path(date_)
        if not os.path.exists(local_file_path):
            return

        uncompressed_path = local_file_path.replace(".gz", "")
        decompressor = zlib.decompressobj(31)
        try:
            with open(local_file_path, "rb") as f_in:
                with open(uncompressed_path, "wb") as f_out:
                    while True:
                        chunk = f_in.read(1024 * 1024)
                        if not chunk:
                            break
                        decompressed_data = decompressor.decompress(chunk)
                        f_out.write(decompressed_data)
                    f_out.write(decompressor.flush())

            lf = pl.scan_csv(uncompressed_path)
            lf = lf.with_columns(
                pl.from_epoch("sip_timestamp", time_unit="ns")
                .dt.replace_time_zone("UTC")
                .dt.convert_time_zone(LOCAL_TIMEZONE_STR)
                .alias("datetime")
            )
            lf = lf.sort(["datetime", "sequence_number"])
            for freq in freqs:
                _partition_path = lambda ctx: os.path.join(
                    self._base_candles_dir,
                    ctx.keys[0].hive_name().split("=")[1],
                    freq,
                    f"{date_.isoformat()}.csv",
                )

                lf.group_by_dynamic("datetime", every=freq, group_by="ticker").agg(
                    pl.col("price").first().alias("open"),
                    pl.col("price").max().alias("high"),
                    pl.col("price").min().alias("low"),
                    pl.col("price").last().alias("close"),
                    pl.col("size").sum().alias("volume"),
                ).with_columns(
                    datetime=pl.col("datetime")
                    .dt.convert_time_zone("UTC")
                    .dt.replace_time_zone(None)
                    .dt.strftime("%Y-%m-%d %H:%M:%S")
                ).sink_csv(
                    pl.PartitionByKey(
                        self._base_candles_dir,
                        file_path=_partition_path,
                        by="ticker",
                        include_key=False,
                    ),
                    mkdir=True,
                )
        finally:
            os.remove(uncompressed_path)

    async def collect(
        self,
        year: int,
        month: int,
        freqs: list[str],
    ):
        today = ClockRegistry.clock.today()
        yday = today - timedelta(days=1)
        start = date(year, month, 1)
        _, monthdays = monthrange(year, month)
        end = min(date(year, month, monthdays), yday)
        await self._collect_files(start, end)

        client = self._client
        self._client = None
        with multiprocessing.Pool(processes=self._max_concurrency) as pool:
            pool.starmap(
                self._process_date,
                [
                    (start + timedelta(days=i), freqs)
                    for i in range((end - start).days + 1)
                ],
            )
        self._client = client

    async def collect_latest(self, freqs: list[str]) -> None:
        downloaded_dates = await self._collect_latest_files()

        client = self._client
        self._client = None
        with multiprocessing.Pool(processes=self._max_concurrency) as pool:
            pool.starmap(
                self._process_date,
                [(date_, freqs) for date_ in downloaded_dates],
            )
        self._client = client
