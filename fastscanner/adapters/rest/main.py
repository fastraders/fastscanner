import multiprocessing
import sys

import uvicorn
from fastapi import APIRouter, FastAPI

from fastscanner.adapters.candle.partitioned_csv import PartitionedCSVCandlesProvider
from fastscanner.adapters.candle.polygon import PolygonCandlesProvider
from fastscanner.adapters.fundamental.eodhd import EODHDFundamentalStore
from fastscanner.adapters.holiday.exchange_calendars import (
    ExchangeCalendarsPublicHolidaysStore,
)
from fastscanner.adapters.realtime.redis_channel import RedisChannel
from fastscanner.pkg import config
from fastscanner.pkg.clock import ClockRegistry, LocalClock
from fastscanner.services.indicators.service import IndicatorsService
from fastscanner.services.registry import ApplicationRegistry
from fastscanner.services.scanners.service import ScannerService

from .indicators import router as indicators_router
from .scanner import router as scanner_router


class FastscannerApp(FastAPI):
    @property
    def indicators(self) -> IndicatorsService:
        return self.state.indicators

    @property
    def scanner(self) -> ScannerService:
        return self.state.scanner

    def startup(self) -> None:
        ClockRegistry.set(LocalClock())

        polygon = PolygonCandlesProvider(
            config.POLYGON_BASE_URL, config.POLYGON_API_KEY
        )
        candles = PartitionedCSVCandlesProvider(polygon)
        fundamental = EODHDFundamentalStore(
            config.EOD_HD_BASE_URL, config.EOD_HD_API_KEY
        )
        holidays = ExchangeCalendarsPublicHolidaysStore()
        channel = RedisChannel(
            unix_socket_path=config.UNIX_SOCKET_PATH,
            host=config.REDIS_DB_HOST,
            port=config.REDIS_DB_PORT,
            password=None,
            db=0,
        )
        self.state.indicators = IndicatorsService(
            candles=candles, fundamentals=fundamental, channel=channel
        )
        self.state.scanner = ScannerService(
            candles=candles, channel=channel, symbols_provider=polygon
        )
        ApplicationRegistry.init(candles, fundamental, holidays)


app = FastscannerApp(docs_url="/api/docs", redoc_url="/api/redoc")

api_router = APIRouter(prefix="/api")
api_router.include_router(indicators_router)
api_router.include_router(scanner_router)
app.include_router(api_router)
app.startup()

if __name__ == "__main__":
    uvicorn.run(
        "fastscanner.adapters.rest.main:app",
        reload=config.DEBUG,
        host="0.0.0.0",
        port=config.SERVER_PORT,
        workers=multiprocessing.cpu_count() * 2 + 1,
    )
