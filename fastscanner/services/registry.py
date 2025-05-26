from typing import TYPE_CHECKING

from .indicators.ports import CandleStore, FundamentalDataStore, PublicHolidaysStore

if TYPE_CHECKING:
    from .indicators.service import IndicatorsService


class ApplicationRegistry:
    candles: CandleStore
    fundamentals: FundamentalDataStore
    holidays: PublicHolidaysStore
    indicators: "IndicatorsService"

    @classmethod
    def init(
        cls,
        candles: CandleStore,
        fundamentals: FundamentalDataStore,
        holidays: PublicHolidaysStore,
    ) -> None:
        cls.candles = candles
        cls.fundamentals = fundamentals
        cls.holidays = holidays

    @classmethod
    def set_indicators(cls, indicators: "IndicatorsService") -> None:
        cls.indicators = indicators

    @classmethod
    def reset(cls) -> None:
        del cls.candles
        del cls.fundamentals
        del cls.holidays
