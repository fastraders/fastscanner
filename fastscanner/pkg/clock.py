import re
from datetime import date, datetime, time, timedelta
from typing import Protocol
from zoneinfo import ZoneInfo

from fastscanner.services.registry import ApplicationRegistry


class Clock(Protocol):
    def now(self) -> datetime: ...


class ClockWrapper(Clock):
    def __init__(self, clock: Clock) -> None:
        self._clock = clock
        self._tzinfo = clock.now().tzinfo

    def market_open(self) -> datetime:
        return self.now().replace(hour=9, minute=30, second=0, microsecond=0)

    def market_close(self) -> datetime:
        return self.now().replace(hour=16, minute=0, second=0, microsecond=0)

    def today(self) -> date:
        return self._clock.now().date()

    def now(self) -> datetime:
        return self._clock.now()

    def today_at(self, time_: time) -> datetime:
        now = self.now()
        return now.replace(
            hour=time_.hour,
            minute=time_.minute,
            second=time_.second,
            microsecond=time_.microsecond,
        )

    def date_at(self, date_: date, time_: time) -> datetime:
        return datetime(
            year=date_.year,
            month=date_.month,
            day=date_.day,
            hour=time_.hour,
            minute=time_.minute,
            second=time_.second,
            microsecond=time_.microsecond,
            tzinfo=self._tzinfo,
        )

    def time_now(self) -> time:
        return self._clock.now().time()

    def is_premarket(self) -> bool:
        return self._clock.now().time() < time(9, 30)

    def is_intraday(self) -> bool:
        return time(9, 30) <= self._clock.now().time() < time(16, 0)

    def is_after_hours(self) -> bool:
        return self._clock.now().time() >= time(16, 0)

    def next_datetime_at(self, at: time) -> datetime:
        now = self.now()
        public_holidays = ApplicationRegistry.holidays.get()
        if now.weekday() >= 5 or now.date() in public_holidays or now.time() >= at:
            next_day = now.date()
            n_days = 1
            while n_days > 0:
                next_day += timedelta(days=1)
                if next_day.weekday() < 5 and next_day not in public_holidays:
                    n_days -= 1

            return datetime.combine(next_day, at)

        return now.replace(
            hour=at.hour,
            minute=at.minute,
            second=at.second,
            microsecond=at.microsecond,
        )


class ClockRegistry:
    clock: ClockWrapper

    @classmethod
    def set(cls, clock: Clock) -> None:
        cls.clock = ClockWrapper(clock)

    @classmethod
    def unset(cls) -> None:
        delattr(cls, "clock")

    @classmethod
    def is_set(cls) -> bool:
        return hasattr(cls, "clock")


class LocalClock:
    def now(self) -> datetime:
        return datetime.now(tz=ZoneInfo(LOCAL_TIMEZONE_STR))


class FixedClock:
    def __init__(self, dt: datetime):
        self._dt = dt

    def now(self) -> datetime:
        return self._dt


LOCAL_TIMEZONE_STR = "America/New_York"
LOCAL_TIMEZONE = ZoneInfo(LOCAL_TIMEZONE_STR)


def split_freq(freq: str) -> tuple[int, str]:
    match = re.match(r"(\d+)(\w+)", freq)
    if match is None:
        raise ValueError(f"Invalid frequency: {freq}")
    mult = int(match.groups()[0])
    unit = match.groups()[1].lower()
    return mult, unit
