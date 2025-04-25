from typing import Protocol

class PolygonRealtime:
    async def subscribe(self, symbols: set[str]): ... # Define if symbols is useful or single symbol instead
    async def unsubscribe(self, symbol: str): ...
    async def start(self): ... # Start the websocket connection
    async def stop(self): ... # Stop the websocket connection
    async def _push(self, df pd.DataFrame): ... # Pushes data to the channel

class Channel(Protocol):
    async def push(self, data: list[dict]): ...

# class PolygonDataProvider(MarketDataProvider, metaclass=SingletonMeta):
#     """
#     Singleton class that asynchronously connects to the Polygon aggregate web sockets,
#     parses and exposes the results in pandas DataFrames.

#     To see a detailed description of the available columns, check https://polygon.io/docs/stocks/ws_stocks_am.

#     Attributes:
#         index_name: The name of the index column in the DataFrame.
#         name_maps: A mapping from the name of the JSON field to the name of the column in the DataFrame.

#     """

#     def __init__(self, channel: Channel):
#         self._channel = channel

#     async def _start(self):
#         async for sock in websockets.connect(settings.POLYGON_WSS):
#             self._socket = sock
#             if self._cancelled:
#                 await sock.close()
#                 break
#             try:
#                 await sock.send(
#                     json.dumps({"action": "auth", "params": settings.POLYGON_API_KEY})
#                 )
#                 await self._subscribe_internal(self._subscribed)
#                 async for msg in sock:
#                     data = json.loads(msg)
#                     self.add_data(data)
#             except (ConnectionClosed, ConnectionResetError, ConnectionClosedError):
#                 continue
