import redis.asyncio as aioredis


class DragonflyCache:
    def __init__(
        self,
        unix_socket_path: str,
        password: str | None,
        db: int,
    ):
        self._client: aioredis.Redis | None = None
        self._unix_socket_path = unix_socket_path
        self._password = password
        self._db = db

    @property
    def client(self) -> aioredis.Redis:
        if self._client is None:
            self._client = aioredis.Redis(
                unix_socket_path=self._unix_socket_path,
                password=self._password,
                db=self._db,
                decode_responses=True,
            )
        return self._client

    async def save(self, key: str, value: str) -> None:
        await self.client.set(key, value)

    async def get(self, key: str) -> str:
        result = await self.client.get(key)
        if result is None:
            raise KeyError(f"Key {key} not found in cache")
        return result

    async def close(self) -> None:
        if self._client is not None:
            await self._client.close()
