import re
from datetime import date, datetime
from typing import Protocol
from zoneinfo import ZoneInfo


class Clock(Protocol):
    def now(self) -> datetime: ...


class ClockWrapper(Clock):
    def __init__(self, clock: Clock) -> None:
        self.clock = clock

    def now(self) -> datetime:
        return self.clock.now()

    def today(self) -> date:
        return self.clock.now().date()

    def market_open(self) -> datetime:
        return self.now().replace(hour=9, minute=30, second=0, microsecond=0)

    def market_close(self) -> datetime:
        return self.now().replace(hour=16, minute=0, second=0, microsecond=0)


class ClockRegistry:
    clock: ClockWrapper

    @classmethod
    def set(cls, clock: Clock) -> None:
        cls.clock = ClockWrapper(clock)

    @classmethod
    def unset(cls) -> None:
        delattr(cls, "clock")


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
