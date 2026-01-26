from datetime import date
from typing import Set

import pytest

from fastscanner.services.indicators.ports import Cache, PublicHolidaysStore
from fastscanner.services.indicators.utils import lookback_days
from fastscanner.services.registry import ApplicationRegistry


class PublicHolidaysTest(PublicHolidaysStore):
    def __init__(self) -> None:
        self._holidays: set[date] = set()

    def set(self, holidays: set[date]) -> None:
        self._holidays = holidays

    def get(self) -> Set[date]:
        return self._holidays


class MockCache(Cache):
    def __init__(self):
        self._data = {}

    async def save(self, key: str, value: str) -> None:
        self._data[key] = value

    async def get(self, key: str) -> str:
        return self._data.get(key, "")


@pytest.fixture
def holidays():
    store = PublicHolidaysTest()
    cache = MockCache()
    ApplicationRegistry.init(candles=None, fundamentals=None, holidays=store, cache=cache)  # type: ignore
    yield store
    ApplicationRegistry.reset()


def test_lookback_days_basic(holidays: PublicHolidaysTest) -> None:
    holidays.set({date(2023, 1, 4), date(2023, 12, 25)})
    start_date = date(2023, 1, 5)  # Thursday
    n_days = 2

    result = lookback_days(start_date, n_days)

    # Jan 4 is a holiday, so we count Jan 3 and Jan 2
    assert result == date(2023, 1, 2)


def test_lookback_days_zero(holidays: PublicHolidaysTest) -> None:
    holidays.set({date(2023, 1, 4)})
    start_date = date(2023, 1, 5)
    n_days = 0

    result = lookback_days(start_date, n_days)

    # With n_days = 0, the function returns the start date
    # This is because the while loop doesn't execute when n_days = 0
    assert result == date(2023, 1, 5)


def test_lookback_days_weekend(holidays: PublicHolidaysTest) -> None:
    holidays.set(set())
    start_date = date(2023, 1, 7)  # Saturday
    n_days = 3

    result = lookback_days(start_date, n_days)

    # Jan 7 (Sat) and Jan 8 (Sun) are skipped
    # Count Jan 6, Jan 5, Jan 4
    assert result == date(2023, 1, 4)


def test_lookback_days_start_on_holiday(holidays: PublicHolidaysTest) -> None:
    holidays.set({date(2023, 1, 5)})
    start_date = date(2023, 1, 5)  # Thursday, holiday
    n_days = 2

    result = lookback_days(start_date, n_days)

    # Jan 5 is a holiday, but the function starts counting from Jan 4
    # So we count Jan 4 and Jan 3
    assert result == date(2023, 1, 3)


def test_lookback_days_consecutive_holidays(holidays: PublicHolidaysTest) -> None:
    holidays.set({date(2023, 1, 2), date(2023, 1, 3), date(2023, 1, 4)})
    start_date = date(2023, 1, 5)  # Thursday
    n_days = 2

    result = lookback_days(start_date, n_days)

    # Jan 4, 3, 2 are holidays, so we count Dec 30 and Dec 29
    # Dec 31 and Jan 1 are weekend days
    assert result == date(2022, 12, 29)


def test_lookback_days_holidays_and_weekends(holidays: PublicHolidaysTest) -> None:
    # Friday Dec 30 and Monday Jan 2 are holidays
    holidays.set({date(2022, 12, 30), date(2023, 1, 2)})
    start_date = date(2023, 1, 3)  # Tuesday
    n_days = 3

    result = lookback_days(start_date, n_days)

    # Dec 30 (Fri) is holiday, Dec 31-Jan 1 is weekend, Jan 2 (Mon) is holiday
    # So we count Dec 29, Dec 28, Dec 27
    assert result == date(2022, 12, 27)


def test_lookback_days_many_days(holidays: PublicHolidaysTest) -> None:
    holidays.set({date(2023, 1, 2), date(2023, 1, 3), date(2023, 1, 4)})
    start_date = date(2023, 1, 5)  # Thursday
    n_days = 100

    result = lookback_days(start_date, n_days)

    assert result == date(2022, 8, 15)
