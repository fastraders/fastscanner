import asyncio
import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any

import pandas as pd

from fastscanner.pkg.datetime import LOCAL_TIMEZONE_STR, split_freq

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

    async def calculate(
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
        days = max(i.lookback_days() for i in ind_instances)
        lagged_start = lookback_days(start, days)

        df = await self.candles.get(symbol, lagged_start, end, freq)
        if df.empty:
            return df

        for indicator in ind_instances:
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


class CandleChannelHandler(ChannelHandler):
    def __init__(
        self,
        symbol: str,
        indicators: list[Indicator],
        handler: SubscriptionHandler,
        freq: str,
    ) -> None:
        self._symbol = symbol
        self._indicators = indicators
        self._handler = handler
        self._freq = freq
        self._buffer = CandleBuffer(symbol, freq, handler, indicators)

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

            await self._buffer.add(new_row)

        except Exception as e:
            logger.error(
                f"[Handler Error] Failed processing message from {channel_id}: {e}"
            )


class CandleBuffer:
    def __init__(
        self,
        symbol: str,
        freq: str,
        handler: SubscriptionHandler,
        indicators: list[Indicator],
    ):
        self.symbol = symbol
        self.freq = freq
        self.handler = handler
        self.indicators = indicators
        self.buffer: dict[pd.Timestamp, pd.Series] = {}
        self.start_time: pd.Timestamp | None = None
        self.expected_count = self._get_expected_count()
        self.timeout_task: asyncio.Task | None = None

    def _get_expected_count(self) -> int:
        n, unit = split_freq(self.freq)
        return n if unit == "min" else n * 60

    def _get_interval_start(self, ts: pd.Timestamp) -> pd.Timestamp:
        freq_minutes = self._get_expected_count()
        minute_group = (ts.minute // freq_minutes) * freq_minutes
        return ts.replace(minute=minute_group, second=0, microsecond=0)

    async def add(self, new_row: pd.Series):
        ts = new_row.name
        interval_start = self._get_interval_start(ts)

        if self.start_time is None or interval_start != self.start_time:
            self.start_time = interval_start
            self.buffer = {}

        self.buffer[ts] = new_row

        if len(self.buffer) == self.expected_count:
            await self._flush()
        elif self.timeout_task is None or self.timeout_task.done():
            self.timeout_task = asyncio.create_task(self._flush_after_timeout())

    async def _flush_after_timeout(self):
        await asyncio.sleep(20)
        await self._flush()

    async def _flush(self):
        if not self.buffer:
            return

        df = pd.DataFrame(self.buffer.values())
        if df.empty:
            return

        agg = pd.Series(
            {
                CandleCol.OPEN: df[CandleCol.OPEN].iloc[0],
                CandleCol.HIGH: df[CandleCol.HIGH].max(),
                CandleCol.LOW: df[CandleCol.LOW].min(),
                CandleCol.CLOSE: df[CandleCol.CLOSE].iloc[-1],
                CandleCol.VOLUME: df[CandleCol.VOLUME].sum(),
            },
            name=self.start_time,
        )

        for ind in self.indicators:
            try:
                agg = await ind.extend_realtime(self.symbol, agg)
            except Exception as e:
                logger.error(f"[{self.symbol}] Indicator error during aggregation: {e}")

        self.handler.handle(self.symbol, agg)
        self.buffer.clear()
        self.start_time = None
