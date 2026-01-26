from typing import TYPE_CHECKING

from .indicators.ports import (
    Cache,
    CandleStore,
    FundamentalDataStore,
    PublicHolidaysStore,
)

if TYPE_CHECKING:
    from .indicators.service import IndicatorsService


class ApplicationRegistry:
    candles: CandleStore
    fundamentals: FundamentalDataStore
    holidays: PublicHolidaysStore
    indicators: "IndicatorsService"
    cache: Cache

    @classmethod
    def init(
        cls,
        candles: CandleStore,
        fundamentals: FundamentalDataStore,
        holidays: PublicHolidaysStore,
        cache: Cache,
    ) -> None:
        cls.candles = candles
        cls.fundamentals = fundamentals
        cls.holidays = holidays
        cls.cache = cache

    @classmethod
    def set_indicators(cls, indicators: "IndicatorsService") -> None:
        cls.indicators = indicators

    @classmethod
    def reset(cls) -> None:
        del cls.candles
        del cls.fundamentals
        del cls.holidays
        del cls.cache

    @classmethod
    def params_for_init(cls) -> dict:
        return {
            "candles": cls.candles,
            "fundamentals": cls.fundamentals,
            "holidays": cls.holidays,
            "cache": cls.cache,
        }
