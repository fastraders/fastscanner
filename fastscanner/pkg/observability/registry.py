import logging
import os
from pathlib import Path
from typing import Literal

from prometheus_client import REGISTRY, CollectorRegistry, multiprocess

logger = logging.getLogger(__name__)

Role = Literal["rest_api", "writer", "caching", "test"]


class _State:
    role: Role | None = None
    scrape_registry: CollectorRegistry | None = None
    multiproc_dir: str | None = None


_state = _State()


def init_metrics(role: Role, multiproc_dir: str | None = None) -> CollectorRegistry:
    if _state.role is not None:
        if _state.role != role:
            raise RuntimeError(
                f"init_metrics already called with role={_state.role!r}; "
                f"cannot re-initialize with role={role!r}"
            )
        assert _state.scrape_registry is not None
        return _state.scrape_registry

    if role == "rest_api":
        from fastscanner.pkg import config

        target_dir = multiproc_dir or config.PROMETHEUS_MULTIPROC_DIR
        Path(target_dir).mkdir(parents=True, exist_ok=True)
        os.environ["PROMETHEUS_MULTIPROC_DIR"] = target_dir
        scrape_registry = CollectorRegistry()
        multiprocess.MultiProcessCollector(scrape_registry)
        _state.scrape_registry = scrape_registry
        _state.multiproc_dir = target_dir
        logger.info(
            "metrics: multiprocess mode initialized (dir=%s)", target_dir
        )
    else:
        _state.scrape_registry = REGISTRY
        logger.info("metrics: single-process mode for role=%s", role)

    _state.role = role
    return _state.scrape_registry


def scrape_registry() -> CollectorRegistry:
    if _state.scrape_registry is None:
        raise RuntimeError("init_metrics() has not been called")
    return _state.scrape_registry


def get_role() -> Role:
    if _state.role is None:
        raise RuntimeError("init_metrics() has not been called")
    return _state.role


def is_multiproc() -> bool:
    return _state.role == "rest_api"


def multiproc_dir() -> str | None:
    return _state.multiproc_dir


def _reset_for_test() -> None:
    _state.role = None
    _state.scrape_registry = None
    _state.multiproc_dir = None
    os.environ.pop("PROMETHEUS_MULTIPROC_DIR", None)
