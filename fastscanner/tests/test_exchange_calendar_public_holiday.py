import json
import os
from datetime import date

import pytest

from fastscanner.adapters.holiday.public_holiday_store import (
    ExchangeCalendarsPublicHolidaysStore,
)


def test_get_holidays_success(tmp_path):
    holidays = [date(2023, 1, 2), date(2023, 12, 25)]
    store = ExchangeCalendarsPublicHolidaysStore("XNYS")
    store.CACHE_DIR = tmp_path

    cache_path = os.path.join(store.CACHE_DIR, "US_exchange_holidays.json")
    os.makedirs(store.CACHE_DIR, exist_ok=True)
    with open(cache_path, "w") as f:
        json.dump([d.isoformat() for d in holidays], f)

    result = store.get()

    assert holidays[0] in result
    assert holidays[1] in result
    assert isinstance(result, set)


def test_get_holidays_fails_on_corrupt_cache(tmp_path):
    store = ExchangeCalendarsPublicHolidaysStore("XNYS")
    store.CACHE_DIR = tmp_path

    cache_path = os.path.join(store.CACHE_DIR, "US_exchange_holidays.json")
    os.makedirs(store.CACHE_DIR, exist_ok=True)

    with open(cache_path, "w") as f:
        f.write("bad-json")

    with pytest.raises(Exception):
        _ = store.get()
