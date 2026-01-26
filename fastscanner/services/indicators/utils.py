from datetime import date, timedelta

import pandas as pd

from ..registry import ApplicationRegistry


def lookback_days(
    start: date,
    n_days: int,
):
    public_holidays = ApplicationRegistry.holidays.get()
    while n_days > 0:
        start -= timedelta(days=1)
        if start.weekday() < 5 and start not in public_holidays:
            n_days -= 1
    return start


def lookforward_days(
    start: date,
    n_days: int,
):
    public_holidays = ApplicationRegistry.holidays.get()
    while n_days > 0:
        start += timedelta(days=1)
        if start.weekday() < 5 and start not in public_holidays:
            n_days -= 1
    return start
