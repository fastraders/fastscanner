import asyncio
import logging
import traceback
from datetime import timedelta

import uvloop

from fastscanner.adapters.candle.polygon import PolygonCandlesProvider
from fastscanner.adapters.realtime.nats_channel import NATSChannel
from fastscanner.adapters.realtime.polygon_realtime import PolygonRealtime
from fastscanner.pkg import config
from fastscanner.pkg.clock import ClockRegistry, LocalClock
from fastscanner.pkg.logging import load_logging_config

load_logging_config()
logging.getLogger().setLevel(logging.DEBUG)
logger = logging.getLogger(__name__)

_DAILY_RESET_HOUR = 3  # 3am local time


def _seconds_until_next_reset() -> float:
    now = ClockRegistry.clock.now()
    next_reset = now.replace(hour=_DAILY_RESET_HOUR, minute=0, second=0, microsecond=0)
    if next_reset <= now:
        next_reset += timedelta(days=1)
    return (next_reset - now).total_seconds()


async def main():
    ClockRegistry.set(LocalClock())
    symbols_provider = PolygonCandlesProvider(
        config.POLYGON_BASE_URL, config.POLYGON_API_KEY
    )

    while True:
        realtime = PolygonRealtime(
            api_key=config.POLYGON_API_KEY,
            channel=NATSChannel(servers=config.NATS_SERVER),
        )
        try:
            await realtime.start()
            await realtime.subscribe_all_active(symbols_provider)
            await asyncio.sleep(_seconds_until_next_reset())
            logger.info("Performing scheduled daily connection reset")
        except Exception as e:
            logger.error(f"Error in main(): {e}")
            logger.error(traceback.format_exc())
        finally:
            realtime.unsubscribe_all()
            await realtime.stop()


if __name__ == "__main__":
    uvloop.run(main())
