import asyncio
import logging
from datetime import datetime
from typing import Any

import pandas as pd

from fastscanner.adapters.candle.partitioned_csv import PartitionedCSVCandlesProvider
from fastscanner.adapters.candle.polygon import PolygonCandlesProvider
from fastscanner.adapters.fundamental.eodhd import EODHDFundamentalStore
from fastscanner.adapters.holiday.exchange_calendars import (
    ExchangeCalendarsPublicHolidaysStore,
)
from fastscanner.adapters.realtime.redis_channel import RedisChannel
from fastscanner.pkg import config
from fastscanner.services.indicators.lib import IndicatorsLibrary
from fastscanner.services.indicators.lib.candle import (
    ATRIndicator,
    CumulativeDailyVolumeIndicator,
    PositionInRangeIndicator,
    PremarketCumulativeIndicator,
)
from fastscanner.services.indicators.lib.daily import (
    DailyATRGapIndicator,
    DailyATRIndicator,
    DailyGapIndicator,
    PrevDayIndicator,
)
from fastscanner.services.indicators.ports import CandleCol
from fastscanner.services.indicators.service import (
    IndicatorParams,
    IndicatorsService,
    SubscriptionHandler,
)
from fastscanner.services.registry import ApplicationRegistry

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class PrintHandler(SubscriptionHandler):
    def handle(self, symbol: str, new_row: dict[str, Any]) -> dict[str, Any]:
        logger.info(f"\n[PrintHandler] Enriched row for {symbol}:")
        logger.info(new_row)
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
    indicators = [
        IndicatorParams(
            type_=prev_indicator.type(), params={"candle_col": CandleCol.CLOSE}
        ),
        IndicatorParams(type_=DailyGapIndicator.type(), params={}),
        IndicatorParams(type_=DailyATRIndicator.type(), params={"period": 14}),
        IndicatorParams(type_=DailyATRGapIndicator.type(), params={"period": 14}),
        IndicatorParams(
            type_=ATRIndicator.type(),
            params={"period": 14, "freq": "1min"},
        ),
        IndicatorParams(type_=CumulativeDailyVolumeIndicator.type(), params={}),
        IndicatorParams(
            type_=PremarketCumulativeIndicator.type(),
            params={"candle_col": CandleCol.CLOSE, "op": "sum"},
        ),
        IndicatorParams(type_=PositionInRangeIndicator.type(), params={"n_days": 5}),
    ]
    for ind in indicators:
        logger.info(
            f"Subscribing to: {ind.type_} -> {IndicatorsLibrary.instance().get(ind.type_, ind.params).column_name()}"
        )
    await service.subscribe_realtime(
        symbol="AAPL",
        freq="1min",
        indicators=indicators,
        handler=PrintHandler(),
    )

    logger.info("\nSubscribed to Redis stream: candles_min_AAPL")

    while True:
        await asyncio.sleep(1)


if __name__ == "__main__":
    asyncio.run(main())
