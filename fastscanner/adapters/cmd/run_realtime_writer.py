import asyncio
import logging
import traceback

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


async def main():
    try:
        # channel = RedisChannel(
        #     unix_socket_path=config.UNIX_SOCKET_PATH,
        #     host=config.REDIS_DB_HOST,
        #     port=config.REDIS_DB_PORT,
        #     password=None,
        #     db=0,
        # )
        ClockRegistry.set(LocalClock())
        symbols_provider = PolygonCandlesProvider(
            config.POLYGON_BASE_URL, config.POLYGON_API_KEY
        )
        channel = NATSChannel(servers=config.NATS_SERVER)
        realtime = PolygonRealtime(
            api_key=config.POLYGON_API_KEY,
            channel=channel,
        )

        await realtime.start()
        await realtime.subscribe_all_active(symbols_provider)

        while True:
            await asyncio.sleep(10.131)  # Just a long sleep. The number is irrelevant.

    except Exception as e:
        logger.error(f"Error in main(): {e}")
        logger.error(traceback.format_exc())
    finally:
        realtime.unsubscribe_all()
        await realtime.stop()


if __name__ == "__main__":
    uvloop.run(main())
