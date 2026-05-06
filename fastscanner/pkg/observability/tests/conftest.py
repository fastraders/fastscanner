import os

import pytest
from prometheus_client import REGISTRY, Counter, Gauge, Histogram
from prometheus_client.registry import CollectorRegistry

from fastscanner.pkg.observability import metrics, registry


def _purge_default_registry() -> None:
    if metrics._instance is None:
        return
    for attr in vars(metrics._instance).values():
        if isinstance(attr, (Counter, Gauge, Histogram)):
            try:
                REGISTRY.unregister(attr)
            except KeyError:
                pass


@pytest.fixture
def fresh_registry() -> CollectorRegistry:
    _purge_default_registry()
    reg = CollectorRegistry()
    metrics._reset_for_test(reg)
    return reg


@pytest.fixture(autouse=True)
def reset_observability_state():
    yield
    _purge_default_registry()
    registry._reset_for_test()
    metrics._instance = None
    os.environ.pop("PROMETHEUS_MULTIPROC_DIR", None)
