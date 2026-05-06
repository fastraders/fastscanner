import pytest

from fastscanner.pkg.observability import metrics, otel_init, registry


@pytest.fixture(autouse=True)
def reset_otel_state():
    otel_init.shutdown()
    metrics._reset_for_test()
    yield
    otel_init.shutdown()
    metrics._reset_for_test()


@pytest.fixture
def in_memory_provider():
    registry.init_metrics(role="test", in_memory=True)
    return otel_init.in_memory_reader()
