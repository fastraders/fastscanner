from .ports import CandleStore, FundamentalDataStore, PublicHolidaysStore


class ApplicationRegistry:
    candles: CandleStore
    fundamentals: FundamentalDataStore
    holidays: PublicHolidaysStore

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
    def reset(cls) -> None:
        del cls.candles
        del cls.fundamentals
        del cls.holidays
