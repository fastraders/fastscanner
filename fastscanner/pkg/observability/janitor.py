import asyncio
import logging
import os
import re
from pathlib import Path

from prometheus_client import multiprocess

from fastscanner.pkg.observability.registry import is_multiproc, multiproc_dir

logger = logging.getLogger(__name__)

_PID_RE = re.compile(r"_(\d+)\.db$")


def _pid_is_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def reap_once() -> int:
    if not is_multiproc():
        return 0
    target = multiproc_dir()
    if target is None:
        return 0
    base = Path(target)
    if not base.is_dir():
        return 0

    pids: set[int] = set()
    for entry in base.iterdir():
        if not entry.is_file():
            continue
        m = _PID_RE.search(entry.name)
        if m is None:
            continue
        pids.add(int(m.group(1)))

    reaped = 0
    for pid in pids:
        if pid == os.getpid():
            continue
        if _pid_is_alive(pid):
            continue
        try:
            multiprocess.mark_process_dead(pid, path=str(base))
            reaped += 1
        except Exception:
            logger.exception("metrics: failed to reap pid=%d", pid)
    if reaped:
        logger.info("metrics: reaped %d orphaned process file set(s)", reaped)
    return reaped


class MetricsJanitor:
    def __init__(self, interval_seconds: int):
        self._interval = interval_seconds
        self._task: asyncio.Task[None] | None = None
        self._stop = asyncio.Event()

    async def _run(self) -> None:
        while not self._stop.is_set():
            try:
                reap_once()
            except Exception:
                logger.exception("metrics: janitor sweep failed")
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self._interval)
            except asyncio.TimeoutError:
                pass

    def start(self) -> None:
        if self._task is not None:
            return
        self._stop.clear()
        self._task = asyncio.create_task(self._run(), name="metrics-janitor")

    async def stop(self) -> None:
        if self._task is None:
            return
        self._stop.set()
        try:
            await asyncio.wait_for(self._task, timeout=5)
        except asyncio.TimeoutError:
            self._task.cancel()
        self._task = None
