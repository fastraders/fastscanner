import asyncio
import logging
from datetime import datetime, time

import uvloop

from fastscanner.adapters.cache.dragonfly import DragonflyCache
from fastscanner.adapters.candle.partitioned_csv import PartitionedCSVCandlesProvider
from fastscanner.adapters.candle.polygon import PolygonCandlesProvider
from fastscanner.adapters.fundamental.eodhd import EODHDFundamentalStore
from fastscanner.adapters.holiday.exchange_calendars import (
    ExchangeCalendarsPublicHolidaysStore,
)
from fastscanner.adapters.realtime.nats_channel import NATSChannel
from fastscanner.pkg import config
from fastscanner.pkg.clock import ClockRegistry, LocalClock
from fastscanner.pkg.logging import load_logging_config
from fastscanner.services.indicators.lib import CacheableIndicator, IndicatorsLibrary
from fastscanner.services.indicators.lib.candle import (
    ATRGapIndicator,
    ATRIndicator,
    CumulativeDailyVolumeIndicator,
    DailyRollingIndicator,
    GapIndicator,
    PositionInRangeIndicator,
    PremarketCumulativeIndicator,
    ShiftIndicator,
)
from fastscanner.services.indicators.lib.daily import (
    ADRIndicator,
    ADVIndicator,
    DailyATRGapIndicator,
    DailyATRIndicator,
    DailyGapIndicator,
    DayOpenIndicator,
    PrevAllDayIndicator,
    PrevDayIndicator,
)
from fastscanner.services.indicators.lib.fundamental import (
    DaysFromEarningsIndicator,
    DaysToEarningsIndicator,
    MarketCapIndicator,
)
from fastscanner.services.indicators.ports import CandleCol
from fastscanner.services.indicators.service import (
    IndicatorParams,
    IndicatorsService,
    SubscriptionHandler,
)
from fastscanner.services.registry import ApplicationRegistry

load_logging_config()
logging.getLogger().setLevel(logging.DEBUG)
logger = logging.getLogger(__name__)


async def main():
    ClockRegistry.set(LocalClock())

    polygon = PolygonCandlesProvider(config.POLYGON_BASE_URL, config.POLYGON_API_KEY)
    candles = PartitionedCSVCandlesProvider(polygon)
    fundamentals = EODHDFundamentalStore(
        config.EOD_HD_BASE_URL,
        config.EOD_HD_API_KEY,
    )
    holidays = ExchangeCalendarsPublicHolidaysStore()
    cache = DragonflyCache(
        unix_socket_path=config.DRAGONFLY_UNIX_SOCKET,
        password=None,
        db=0,
    )
    channel = NATSChannel(servers=config.NATS_SERVER)

    ApplicationRegistry.init(candles, fundamentals, holidays, cache)
    indicators_service = IndicatorsService(
        candles,
        fundamentals,
        channel,
        cache,
        config.NATS_SYMBOL_SUBSCRIBE_CHANNEL,
        config.NATS_SYMBOL_UNSUBSCRIBE_CHANNEL,
        config.CACHE_AT_SECONDS,
    )
    ApplicationRegistry.set_indicators(indicators_service)

    indicators_with_cache: list[CacheableIndicator] = [
        ADVIndicator(period=14),
        ADRIndicator(period=14),
        MarketCapIndicator(),
        DailyRollingIndicator(n_days=20, operation="min", candle_col=CandleCol.LOW),
        DailyRollingIndicator(n_days=20, operation="max", candle_col=CandleCol.HIGH),
        DaysFromEarningsIndicator(),
        PremarketCumulativeIndicator(CandleCol.VOLUME, "sum"),
        DailyATRIndicator(14),
        PrevDayIndicator(CandleCol.CLOSE),
        DayOpenIndicator(),
    ]
    logger.info(f"Loading {len(indicators_with_cache)} indicators from cache.")
    for i in indicators_with_cache:
        try:
            await i.load_from_cache()
        except KeyError:
            await i.save_to_cache()
            logger.info(f"Indicator {i} not found in cache. It will be cached anew.")
    sub_id = await indicators_service.cache_indicators(indicators_with_cache)
    logger.info(f"Subscribed to all symbols. Starting caching loop...")
    try:
        while True:
            await asyncio.sleep(20.1242)  # Sleep a random time to avoid thundering herd
    finally:
        await indicators_service.stop_caching(sub_id)
        await cache.close()


if __name__ == "__main__":
    uvloop.run(main())
