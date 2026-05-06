import asyncio
import os
from pathlib import Path

import pytest
from prometheus_client import CollectorRegistry

from fastscanner.pkg.observability import janitor, metrics, registry


def _write_proc_file(base: Path, kind: str, pid: int) -> Path:
    f = base / f"{kind}_{pid}.db"
    f.write_bytes(b"\x00" * 1024)
    return f


def test_reap_once_noop_when_not_multiproc(tmp_path: Path):
    registry.init_metrics(role="writer")
    assert janitor.reap_once() == 0


def test_reap_once_skips_alive_pids(tmp_path: Path):
    registry.init_metrics(role="rest_api", multiproc_dir=str(tmp_path))
    metrics._reset_for_test(CollectorRegistry())
    alive_file = _write_proc_file(tmp_path, "counter", os.getpid())
    assert janitor.reap_once() == 0
    assert alive_file.exists()


def test_reap_once_removes_orphan_live_gauges(tmp_path: Path):
    registry.init_metrics(role="rest_api", multiproc_dir=str(tmp_path))
    metrics._reset_for_test(CollectorRegistry())
    dead_pid = _find_dead_pid()
    gauge_livesum = _write_proc_file(tmp_path, "gauge_livesum", dead_pid)
    gauge_liveall = _write_proc_file(tmp_path, "gauge_liveall", dead_pid)
    counter = _write_proc_file(tmp_path, "counter", dead_pid)
    histogram = _write_proc_file(tmp_path, "histogram", dead_pid)

    reaped = janitor.reap_once()

    assert reaped == 1
    assert not gauge_livesum.exists()
    assert not gauge_liveall.exists()
    assert counter.exists()
    assert histogram.exists()


def test_reap_once_ignores_unrelated_files(tmp_path: Path):
    registry.init_metrics(role="rest_api", multiproc_dir=str(tmp_path))
    metrics._reset_for_test(CollectorRegistry())
    junk = tmp_path / "README.md"
    junk.write_text("not a prom file")
    assert janitor.reap_once() == 0
    assert junk.exists()


def test_pid_is_alive_for_self():
    assert janitor._pid_is_alive(os.getpid()) is True


def test_pid_is_alive_for_dead_pid():
    assert janitor._pid_is_alive(_find_dead_pid()) is False


@pytest.mark.asyncio
async def test_metrics_janitor_runs_periodically(tmp_path: Path):
    registry.init_metrics(role="rest_api", multiproc_dir=str(tmp_path))
    metrics._reset_for_test(CollectorRegistry())
    dead_pid = _find_dead_pid()
    gauge_file = _write_proc_file(tmp_path, "gauge_livesum", dead_pid)
    j = janitor.MetricsJanitor(interval_seconds=60)
    j.start()
    await asyncio.sleep(0.05)
    await j.stop()
    assert not gauge_file.exists()


@pytest.mark.asyncio
async def test_metrics_janitor_double_start_idempotent(tmp_path: Path):
    registry.init_metrics(role="rest_api", multiproc_dir=str(tmp_path))
    j = janitor.MetricsJanitor(interval_seconds=60)
    j.start()
    first = j._task
    j.start()
    assert j._task is first
    await j.stop()


def _find_dead_pid() -> int:
    for pid in range(99000, 99500):
        if not janitor._pid_is_alive(pid):
            return pid
    raise AssertionError("could not find a dead pid in scan range")
