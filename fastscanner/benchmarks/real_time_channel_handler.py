import asyncio
import logging
from datetime import datetime

import pandas as pd

from fastscanner.adapters.candle.partitioned_csv import PartitionedCSVCandlesProvider
from fastscanner.adapters.candle.polygon import PolygonCandlesProvider
from fastscanner.adapters.fundamental.eodhd import EODHDFundamentalStore
from fastscanner.adapters.holiday.exchange_calendars import (
    ExchangeCalendarsPublicHolidaysStore,
)
from fastscanner.adapters.realtime.redis_channel import RedisChannel
from fastscanner.pkg import config
from fastscanner.services.indicators.lib.daily import (
    DailyGapIndicator,
    PrevDayIndicator,
)
from fastscanner.services.indicators.ports import CandleCol
from fastscanner.services.indicators.registry import ApplicationRegistry
from fastscanner.services.indicators.service import (
    IndicatorParams,
    IndicatorsService,
    SubscriptionHandler,
)

logging.basicConfig(level=logging.INFO)


class PrintHandler(SubscriptionHandler):
    def handle(self, symbol: str, new_row: pd.Series) -> pd.Series:
        print(f"\n[PrintHandler] Enriched row for {symbol}:")
        for k, v in new_row.items():
            print(f"  {k}: {v}")
        return new_row


async def main():
    redis_channel = RedisChannel(
        host=config.REDIS_DB_HOST,
        port=config.REDIS_DB_PORT,
        unix_socket_path=config.UNIX_SOCKET_PATH,
        password=None,
        db=0,
    )
    polygon = PolygonCandlesProvider(config.POLYGON_BASE_URL, config.POLYGON_API_KEY)
    holidays = ExchangeCalendarsPublicHolidaysStore()
    candles = PartitionedCSVCandlesProvider(polygon)
    fundamentals = EODHDFundamentalStore(config.EOD_HD_BASE_URL, config.EOD_HD_API_KEY)
    service = IndicatorsService(
        candles=candles,
        fundamentals=fundamentals,
        channel=redis_channel,
    )
    ApplicationRegistry.init(candles, fundamentals, holidays)
    prev_indicator = PrevDayIndicator(candle_col=CandleCol.CLOSE)
    daily_gap_indicator = DailyGapIndicator()
    indicators = [
        IndicatorParams(
            type_=prev_indicator.type(), params={"candle_col": CandleCol.CLOSE}
        ),
        IndicatorParams(type_=daily_gap_indicator.type(), params={}),
    ]

    await service.subscribe_realtime(
        symbol="AAPL",
        freq="1min",
        indicators=indicators,
        handler=PrintHandler(),
    )

    print("\nSubscribed to Redis stream: candles_min_AAPL")

    while True:
        await asyncio.sleep(1)


if __name__ == "__main__":
    asyncio.run(main())
