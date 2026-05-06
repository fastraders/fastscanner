import os
from pathlib import Path

import pytest
from prometheus_client import REGISTRY, CollectorRegistry

from fastscanner.pkg.observability import registry


def test_init_writer_uses_default_registry():
    reg = registry.init_metrics(role="writer")
    assert reg is REGISTRY
    assert registry.get_role() == "writer"
    assert registry.is_multiproc() is False
    assert registry.multiproc_dir() is None


def test_init_caching_uses_default_registry():
    reg = registry.init_metrics(role="caching")
    assert reg is REGISTRY
    assert registry.is_multiproc() is False


def test_init_rest_api_creates_multiproc_collector(tmp_path: Path):
    reg = registry.init_metrics(role="rest_api", multiproc_dir=str(tmp_path))
    assert isinstance(reg, CollectorRegistry)
    assert reg is not REGISTRY
    assert registry.is_multiproc() is True
    assert registry.multiproc_dir() == str(tmp_path)


def test_init_rest_api_creates_dir_if_missing(tmp_path: Path):
    target = tmp_path / "nested" / "fs-prom"
    assert not target.exists()
    registry.init_metrics(role="rest_api", multiproc_dir=str(target))
    assert target.is_dir()


def test_init_idempotent_with_same_role(tmp_path: Path):
    a = registry.init_metrics(role="rest_api", multiproc_dir=str(tmp_path))
    b = registry.init_metrics(role="rest_api", multiproc_dir=str(tmp_path))
    assert a is b


def test_init_rejects_role_change():
    registry.init_metrics(role="writer")
    with pytest.raises(RuntimeError, match="cannot re-initialize"):
        registry.init_metrics(role="caching")


def test_scrape_registry_before_init_raises():
    with pytest.raises(RuntimeError):
        registry.scrape_registry()


def test_get_role_before_init_raises():
    with pytest.raises(RuntimeError):
        registry.get_role()


def test_is_multiproc_false_before_init():
    assert registry.is_multiproc() is False
