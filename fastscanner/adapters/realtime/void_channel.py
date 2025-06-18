import asyncio
import logging
from typing import Any

from fastscanner.services.indicators.ports import ChannelHandler

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class VoidChannel:
    async def push(self, channel_id: str, data: dict[Any, Any], flush: bool = True): ...

    async def flush(self): ...

    async def subscribe(self, channel_id: str, handler: ChannelHandler) -> None: ...

    async def unsubscribe(self, channel_id: str, handler: ChannelHandler) -> None: ...
