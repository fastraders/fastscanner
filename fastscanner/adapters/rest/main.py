import multiprocessing
from contextlib import asynccontextmanager
from typing import AsyncIterator, TypedDict

import uvicorn
from fastapi import APIRouter, FastAPI

from fastscanner.adapters.cache.dragonfly import DragonflyCache
from fastscanner.adapters.candle.partitioned_csv import PartitionedCSVCandlesProvider
from fastscanner.adapters.candle.polygon import PolygonCandlesProvider
from fastscanner.adapters.fundamental.eodhd import EODHDFundamentalStore
from fastscanner.adapters.holiday.exchange_calendars import (
    ExchangeCalendarsPublicHolidaysStore,
)
from fastscanner.adapters.realtime.nats_channel import NATSChannel
from fastscanner.adapters.realtime.redis_channel import RedisChannel
from fastscanner.pkg import config
from fastscanner.pkg.clock import ClockRegistry, LocalClock
from fastscanner.pkg.logging import load_logging_config
from fastscanner.services.indicators.service import IndicatorsService
from fastscanner.services.registry import ApplicationRegistry
from fastscanner.services.scanners.service import ScannerService

from .indicators import router as indicators_router
from .init import init
from .scanner import router as scanner_router


class State(TypedDict):
    indicators: IndicatorsService
    scanner: ScannerService


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[State]:
    indicators_service, scanner_service = init()

    yield {
        "indicators": indicators_service,
        "scanner": scanner_service,
    }

    await indicators_service.stop()


load_logging_config()
app = FastAPI(docs_url="/api/docs", redoc_url="/api/redoc", lifespan=lifespan)

api_router = APIRouter(prefix="/api")
api_router.include_router(indicators_router)
api_router.include_router(scanner_router)
app.include_router(api_router)

if __name__ == "__main__":
    uvicorn.run(
        "fastscanner.adapters.rest.main:app",
        reload=config.DEBUG,
        host="0.0.0.0",
        port=config.SERVER_PORT,
        workers=multiprocessing.cpu_count() * 2 + 1,
        ws_ping_interval=config.WS_PING_INTERVAL,
        ws_ping_timeout=config.WS_PING_TIMEOUT,
    )
