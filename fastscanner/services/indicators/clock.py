from datetime import date, datetime
from typing import Protocol


class Clock(Protocol):
    def now(self) -> datetime:
        ...


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
