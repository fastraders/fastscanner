import json
import logging
import os
from dataclasses import dataclass
from datetime import date as Date
from typing import Awaitable, Callable
from urllib.parse import urljoin

import httpx
import pandas as pd

from fastscanner.pkg.clock import LOCAL_TIMEZONE_STR, ClockRegistry
from fastscanner.pkg.http import async_retry_request
from fastscanner.services.indicators.ports import CandleCol as C
from fastscanner.services.indicators.ports import CandleStore

logger = logging.getLogger(__name__)


@dataclass
class Split:
    execution_date: Date
    historical_adjustment_factor: float
    split_from: int
    split_to: int

    def execution_ts(self) -> pd.Timestamp:
        return pd.Timestamp(self.execution_date, tz=LOCAL_TIMEZONE_STR)


class _MassiveSplitsLoader:
    _base_dir: str
    _splits_cache: dict[str, list[Split]] | None = None

    @property
    def splits(self) -> "dict[str, list[Split]]":
        if self._splits_cache is None:
            self._splits_cache = self._load_splits()
        return self._splits_cache

    def _splits_path(self) -> str:
        return os.path.join(self._base_dir, "polygon_splits.json")

    def _load_splits(self) -> "dict[str, list[Split]]":
        with open(self._splits_path(), "r") as f:
            return {
                symbol: [
                    Split(
                        execution_date=Date.fromisoformat(s["execution_date"]),
                        historical_adjustment_factor=s["historical_adjustment_factor"],
                        split_from=s["split_from"],
                        split_to=s["split_to"],
                    )
                    for s in splits
                ]
                for symbol, splits in json.load(f).items()
            }


class MassiveAdjustedCollector(_MassiveSplitsLoader):
    def __init__(
        self,
        base_dir: str,
        api_key: str,
        base_url: str,
    ) -> None:
        self._base_dir = base_dir
        self._api_key = api_key
        self._base_url = base_url

    async def _fetch_splits(
        self, extra_params: dict | None = None
    ) -> "dict[str, list[Split]]":
        async with httpx.AsyncClient() as client:
            url = urljoin(self._base_url, "stocks/v1/splits")
            params = {
                "apiKey": self._api_key,
                "limit": 5000,  # Maximum allowed by Massive API
                "sort": "execution_date.asc",
                "execution_date.lte": ClockRegistry.clock.today().isoformat(),
                **(extra_params or {}),
            }
            splits: dict[str, list[Split]] = {}
            while True:
                response = await async_retry_request(
                    client,
                    "GET",
                    url,
                    params=params,
                )
                response.raise_for_status()
                data = response.json()
                if len(data.get("results", [])) == 0:
                    break
                for item in data["results"]:
                    if "historical_adjustment_factor" not in item:
                        continue
                    ticker = item["ticker"]
                    splits.setdefault(ticker, []).append(
                        Split(
                            execution_date=Date.fromisoformat(item["execution_date"]),
                            historical_adjustment_factor=item[
                                "historical_adjustment_factor"
                            ],
                            split_from=item["split_from"],
                            split_to=item["split_to"],
                        )
                    )

                if data.get("next_url") is None:
                    break
                url = f'{data["next_url"]}&apiKey={self._api_key}'
                params = None

        return splits

    async def collect_all_splits(self) -> None:
        logger.info("Collecting all splits from Polygon")
        splits = await self._fetch_splits({"execution_date.gte": f"2000-01-01"})
        self._save_splits(splits)
        self._splits_cache = splits
        logger.info(f"Collected splits for {len(splits)} symbols")

    async def collect_latest_splits(self) -> None:
        try:
            all_splits = self.splits
        except FileNotFoundError:
            logger.info("No existing splits cache found. Collecting all splits.")
            await self.collect_all_splits()
            all_splits = self.splits
        latest_date = max(
            (splits[-1].execution_date for splits in all_splits.values() if splits)
        )
        extra_params = {"execution_date.gt": latest_date.isoformat()}
        new_splits = await self._fetch_splits(extra_params=extra_params)
        for symbol, _ in new_splits.items():
            # Needs to retrieve again all splits for symbol to update the historical_adjustment_factor
            all_symbol_splits = await self._fetch_splits({"ticker": symbol})
            all_splits[symbol] = all_symbol_splits.get(symbol, [])

        self._save_splits(all_splits)

    def _save_splits(self, splits: "dict[str, list[Split]]") -> None:
        with open(self._splits_path(), "w") as f:
            json.dump(
                {
                    symbol: [
                        {
                            "execution_date": s.execution_date.isoformat(),
                            "historical_adjustment_factor": s.historical_adjustment_factor,
                            "split_from": s.split_from,
                            "split_to": s.split_to,
                        }
                        for s in splits
                    ]
                    for symbol, splits in splits.items()
                },
                f,
            )


class MassiveAdjustedMixin(_MassiveSplitsLoader):
    get: Callable[[str, Date, Date, str], Awaitable[pd.DataFrame]]

    def adjust(self, symbol: str, df: pd.DataFrame, to: Date) -> pd.DataFrame:
        if df.empty:
            return df

        min_date = df.index.min().date()
        splits: list[Split] = [
            s for s in self.splits.get(symbol, []) if min_date <= s.execution_date <= to
        ]
        if len(splits) == 0:
            return df

        prev_split = splits[0]
        assert isinstance(df.index, pd.DatetimeIndex)
        price_cols = [C.OPEN, C.HIGH, C.LOW, C.CLOSE]
        indexer = df.index < prev_split.execution_ts()
        df.loc[indexer, price_cols] *= prev_split.split_from / prev_split.split_to

        for split in splits[1:]:
            ts = split.execution_ts()
            indexer = df.index < ts
            df.loc[indexer, price_cols] *= split.split_from / split.split_to
            prev_split = split

        return df
