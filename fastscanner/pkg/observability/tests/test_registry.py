import pytest

from fastscanner.pkg.observability import otel_init, registry


def test_init_writer_role():
    registry.init_metrics(role="writer")
    assert registry.is_initialized()
    assert registry.get_role() == "writer"


def test_init_caching_role():
    registry.init_metrics(role="caching")
    assert registry.get_role() == "caching"


def test_init_rest_api_role():
    registry.init_metrics(role="rest_api")
    assert registry.get_role() == "rest_api"


def test_init_in_memory_creates_reader():
    registry.init_metrics(role="test", in_memory=True)
    reader = otel_init.in_memory_reader()
    assert reader is not None


def test_init_idempotent_with_same_role():
    registry.init_metrics(role="writer")
    registry.init_metrics(role="writer")
    assert registry.get_role() == "writer"


def test_init_rejects_role_change():
    registry.init_metrics(role="writer")
    with pytest.raises(RuntimeError, match="cannot re-initialize"):
        registry.init_metrics(role="caching")


def test_get_role_before_init_raises():
    with pytest.raises(RuntimeError, match="setup_meter_provider"):
        registry.get_role()


def test_is_initialized_returns_false_before_init():
    assert not registry.is_initialized()
