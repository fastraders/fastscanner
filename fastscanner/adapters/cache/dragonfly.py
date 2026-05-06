import time

import redis.asyncio as aioredis

from fastscanner.pkg.observability import metrics


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
        start = time.perf_counter()
        outcome = "ok"
        try:
            await self.client.set(key, value)
        except Exception:
            outcome = "error"
            raise
        finally:
            metrics.dragonfly_command("set", outcome, time.perf_counter() - start)

    async def get(self, key: str) -> str:
        start = time.perf_counter()
        outcome = "hit"
        try:
            result = await self.client.get(key)
        except Exception:
            outcome = "error"
            metrics.dragonfly_command("get", outcome, time.perf_counter() - start)
            raise
        if result is None:
            outcome = "miss"
            metrics.dragonfly_command("get", outcome, time.perf_counter() - start)
            raise KeyError(f"Key {key} not found in cache")
        metrics.dragonfly_command("get", outcome, time.perf_counter() - start)
        return result

    async def close(self) -> None:
        if self._client is not None:
            await self._client.close()
