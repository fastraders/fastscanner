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
from fastscanner.services.indicators.service import IndicatorsService
from fastscanner.services.registry import ApplicationRegistry
from fastscanner.services.scanners.service import ScannerService


def init() -> tuple[IndicatorsService, ScannerService]:
    ClockRegistry.set(LocalClock())

    polygon = PolygonCandlesProvider(config.POLYGON_BASE_URL, config.POLYGON_API_KEY)
    candles = PartitionedCSVCandlesProvider(polygon)
    fundamental = EODHDFundamentalStore(config.EOD_HD_BASE_URL, config.EOD_HD_API_KEY)
    holidays = ExchangeCalendarsPublicHolidaysStore()
    channel = NATSChannel(servers=config.NATS_SERVER)
    cache = DragonflyCache(
        config.DRAGONFLY_UNIX_SOCKET,
        password=None,
        db=0,
    )
    indicators_service = IndicatorsService(
        candles=candles,
        fundamentals=fundamental,
        channel=channel,
        cache=cache,
        symbols_subscribe_channel=config.NATS_SYMBOL_SUBSCRIBE_CHANNEL,
        symbols_unsubscribe_channel=config.NATS_SYMBOL_UNSUBSCRIBE_CHANNEL,
        cache_at_seconds=config.CACHE_AT_SECONDS,
        symbols_news_subscribe_channel=config.NATS_SYMBOL_NEWS_SUBSCRIBE_CHANNEL,
        symbols_news_unsubscribe_channel=config.NATS_SYMBOL_NEWS_UNSUBSCRIBE_CHANNEL,
    )
    scanner_service = ScannerService(
        candles=candles, channel=channel, symbols_provider=polygon
    )
    ApplicationRegistry.init(candles, fundamental, holidays, cache)
    return indicators_service, scanner_service
