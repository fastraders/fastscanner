import json
import logging
import os
from datetime import date, datetime
from typing import Set

import exchange_calendars as ecals
import pandas as pd

logger = logging.getLogger(__name__)


class ExchangeCalendarsPublicHolidaysStore:
    CACHE_DIR = os.path.join("data", "holidays")

    def __init__(self, exchange_code: str = "XNAS"):
        self._exchange_code = exchange_code
        os.makedirs(self.CACHE_DIR, exist_ok=True)
        self._holidays: Set[date] | None = None

    def get(self) -> Set[date]:
        if self._holidays is None:
            self._holidays = self._load_or_fetch()
        return self._holidays

    def _load_or_fetch(self) -> Set[date]:
        path = self._get_cache_path()
        if os.path.exists(path):
            logger.info(f"Loading holidays from cache for {self._exchange_code}")
            return self._load_cache(path)

        logger.info(
            f"Fetching holidays from exchange-calendars for {self._exchange_code}"
        )
        calendar = ecals.get_calendar(self._exchange_code)

        adhoc = calendar.adhoc_holidays
        regular = (
            calendar.regular_holidays.holidays()
            if calendar.regular_holidays
            else pd.DatetimeIndex([])
        )
        all_holidays = pd.DatetimeIndex(
            sorted(set(adhoc).union(set(regular)))
        ).tz_localize(None)

        holidays = {d.date() for d in all_holidays}
        self._store_cache(holidays)
        return holidays

    def _store_cache(self, holidays: Set[date]) -> None:
        with open(self._get_cache_path(), "w") as f:
            json.dump([d.isoformat() for d in sorted(holidays)], f, indent=2)

    def _load_cache(self, path: str) -> Set[date]:
        with open(path, "r") as f:
            return {datetime.strptime(d, "%Y-%m-%d").date() for d in json.load(f)}

    def _get_cache_path(self) -> str:
        return os.path.join(self.CACHE_DIR, f"US_Exchange_holidays.json")
