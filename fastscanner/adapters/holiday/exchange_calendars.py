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
        self._holidays: Set[date] | None = None

    def get(self) -> Set[date]:
        if self._holidays is None:
            self._holidays = self._load_or_fetch()
        return self._holidays

    def reload(self) -> Set[date]:
        self._holidays = self._load_or_fetch(force_reload=True)
        return self._holidays

    def _load_or_fetch(self, force_reload: bool = False) -> Set[date]:
        os.makedirs(self.CACHE_DIR, exist_ok=True)
        path = self._get_cache_path()

        if not force_reload and os.path.exists(path):
            logger.info(f"Loading holidays from cache for {self._exchange_code}")
            return self._load_cache(path)

        logger.info(
            f"Fetching holidays from exchange-calendars for {self._exchange_code}"
        )
        calendar = ecals.get_calendar(self._exchange_code)

        adhoc: Set[date] = {h.date() for h in calendar.adhoc_holidays}

        assert calendar.regular_holidays is not None
        regular: Set[date] = {h.date() for h in calendar.regular_holidays.holidays()}

        holidays = adhoc.union(regular)

        self._store_cache(holidays)
        return holidays

    def _store_cache(self, holidays: Set[date]) -> None:
        with open(self._get_cache_path(), "w") as f:
            json.dump([d.isoformat() for d in sorted(holidays)], f, indent=2)

    def _load_cache(self, path: str) -> Set[date]:
        with open(path, "r") as f:
            return {datetime.strptime(d, "%Y-%m-%d").date() for d in json.load(f)}

    def _get_cache_path(self) -> str:
        return os.path.join(self.CACHE_DIR, f"US_exchange_holidays.json")
