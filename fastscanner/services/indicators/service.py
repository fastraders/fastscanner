import asyncio
import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any

import pandas as pd

from fastscanner.pkg.candle import CandleBuffer
from fastscanner.pkg.datetime import LOCAL_TIMEZONE_STR, split_freq
from fastscanner.services.indicators.clock import ClockRegistry

from .lib import Indicator, IndicatorsLibrary
from .ports import (
    CandleCol,
    CandleStore,
    Channel,
    ChannelHandler,
    FundamentalDataStore,
    PublicHolidaysStore,
)
from .utils import lookback_days

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


@dataclass
class IndicatorParams:
    type_: str
    params: dict[str, Any]


class IndicatorsService:
    def __init__(
        self,
        candles: CandleStore,
        fundamentals: FundamentalDataStore,
        channel: Channel,
    ) -> None:
        self.candles = candles
        self.fundamentals = fundamentals
        self.channel = channel

    async def calculate_from_params(
        self,
        symbol: str,
        start: date,
        end: date,
        freq: str,
        indicators: list[IndicatorParams],
    ) -> pd.DataFrame:
        ind_instances = [
            IndicatorsLibrary.instance().get(i.type_, i.params) for i in indicators
        ]
        return await self.calculate(symbol, start, end, freq, ind_instances)

    async def calculate(
        self,
        symbol: str,
        start: date,
        end: date,
        freq: str,
        indicators: list[Indicator],
    ) -> pd.DataFrame:
        days = max(ind.lookback_days() for ind in indicators)
        lagged_start = lookback_days(start, days)

        df = await self.candles.get(symbol, lagged_start, end, freq)
        if df.empty:
            return df

        for indicator in indicators:
            df = await indicator.extend(symbol, df)
        return df.loc[df.index.date >= start]  # type: ignore

    async def subscribe_realtime(
        self,
        symbol: str,
        freq: str,
        indicators: list[IndicatorParams],
        handler: "SubscriptionHandler",
    ):
        """
        Redis -> RedisChannel -> For all subscribers, compute the indicators -> SubscriptionHandler
            candle                                                          candle with indicators
        Store the subscription handler in a dictionary.
        Every time we get a new candle, for the symbol, we will first fill the new row with the indicators (extend_realtime).
        Then we will call the handler with the new row.
        The first time you get a subscription to a symbol, you need to subscribe to the channel.
        """
        indicator_instances = [
            IndicatorsLibrary.instance().get(i.type_, i.params) for i in indicators
        ]

        max_days = max(ind.lookback_days() for ind in indicator_instances)
        today = datetime.now().date()
        if max_days > 0:
            lookback_start = lookback_days(today, max_days)
            end_date = today - timedelta(days=1)

            df = await self.candles.get(symbol, lookback_start, end_date, freq)
            if df.empty:
                logger.warning(
                    f"No historical data found for {symbol} from {lookback_start} to {end_date}"
                )
                return

            for _, row in df.iterrows():
                for ind in indicator_instances:
                    row = await ind.extend_realtime(symbol, row)

        stream_key = f"candles_min_{symbol}"
        await self.channel.subscribe(
            stream_key, CandleChannelHandler(symbol, indicator_instances, handler, freq)
        )


class SubscriptionHandler:
    def handle(self, symbol: str, new_row: pd.Series) -> pd.Series: ...


class CandleChannelHandler:
    def __init__(
        self,
        symbol: str,
        indicators: list[Indicator],
        handler: SubscriptionHandler,
        freq: str,
        candle_timeout: float = 20,
    ) -> None:
        self._symbol = symbol
        self._indicators = indicators
        self._handler = handler
        self._freq = freq
        self._buffer = CandleBuffer(symbol, freq, candle_timeout)
        self._candle_timeout = candle_timeout
        self._buffer_lock = asyncio.Lock()

    async def handle(self, channel_id: str, data: dict[Any, Any]) -> None:
        try:
            for field in (
                CandleCol.OPEN,
                CandleCol.HIGH,
                CandleCol.LOW,
                CandleCol.CLOSE,
                CandleCol.VOLUME,
            ):
                if field in data:
                    data[field] = float(data[field])

            if "timestamp" not in data:
                logger.warning(f"Missing timestamp in message from {channel_id}")
                return

            ts = pd.to_datetime(int(data["timestamp"]), unit="ms", utc=True).tz_convert(
                LOCAL_TIMEZONE_STR
            )
            new_row = pd.Series(data, name=ts)

            if self._freq == "1min":
                for ind in self._indicators:
                    new_row = await ind.extend_realtime(self._symbol, new_row)
                self._handler.handle(self._symbol, new_row)
                return
            agg = await self._buffer.add(new_row)
            if agg is None:
                 return
            for ind in self._indicators:
                agg = await ind.extend_realtime(self._symbol, agg)
 
            self._handler.handle(self._symbol, agg)

        except Exception as e:
            logger.exception(
                f"[Handler Error] Failed processing message from {channel_id}: {e}"
            )
