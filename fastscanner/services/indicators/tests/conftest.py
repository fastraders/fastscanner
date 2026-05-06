import pytest
from prometheus_client import REGISTRY, CollectorRegistry, Counter, Gauge, Histogram

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


@pytest.fixture(autouse=True)
def _isolate_metrics():
    _purge_default_registry()
    fresh = CollectorRegistry()
    metrics._reset_for_test(fresh)
    yield fresh
    _purge_default_registry()
    registry._reset_for_test()
    metrics._instance = None
