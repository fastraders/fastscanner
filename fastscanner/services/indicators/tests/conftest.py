import pytest

from fastscanner.pkg.observability import metrics, otel_init, registry


@pytest.fixture(autouse=True)
def _isolate_metrics():
    otel_init.shutdown()
    metrics._reset_for_test()
    registry.init_metrics(role="test", in_memory=True)
    yield otel_init.in_memory_reader()
    otel_init.shutdown()
    metrics._reset_for_test()
