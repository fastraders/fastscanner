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
from fastscanner.pkg import config
from fastscanner.services.indicators.registry import ApplicationRegistry
from fastscanner.services.indicators.service import IndicatorsService


class FastscannerApp(FastAPI):
    @property
    def indicators(self) -> IndicatorsService:
        return self.state.indicators

    def startup(self) -> None:
        polygon = PolygonCandlesProvider(
            config.POLYGON_BASE_URL, config.POLYGON_API_KEY
        )
        candles = PartitionedCSVCandlesProvider(polygon)
        fundamental = EODHDFundamentalStore(
            config.EOD_HD_BASE_URL, config.EOD_HD_API_KEY
        )
        holidays = ExchangeCalendarsPublicHolidaysStore()
        self.state.indicators = IndicatorsService(
            candles=candles,
            fundamentals=fundamental,
        )
        ApplicationRegistry.init(candles, fundamental, holidays)


app = FastscannerApp(docs_url="/api/docs", redoc_url="/api/redoc")

api_router = APIRouter(prefix="/api")
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
