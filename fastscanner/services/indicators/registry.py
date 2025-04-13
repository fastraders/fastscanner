from .ports import CandleStore, FundamentalDataStore


class ApplicationRegistry:
    candles: CandleStore
    fundamentals: FundamentalDataStore

    @classmethod
    def init(
        cls,
        candles: CandleStore,
        fundamentals: FundamentalDataStore,
    ) -> None:
        cls.candles = candles
        cls.fundamentals = fundamentals
