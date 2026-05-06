import socket
import urllib.request

import pytest
from prometheus_client import CollectorRegistry

from fastscanner.pkg.observability import metrics, registry, server


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def test_start_metrics_server_serves_exposition(fresh_registry: CollectorRegistry):
    registry.init_metrics(role="writer")
    metrics._reset_for_test(registry.scrape_registry())
    metrics.candle_received("Stocks")
    metrics.set_ws_connected(True)
    port = _free_port()
    handle = server.start_metrics_server(port=port, host="127.0.0.1")
    try:
        assert handle.port == port
        with urllib.request.urlopen(
            f"http://127.0.0.1:{port}/metrics", timeout=2
        ) as resp:
            body = resp.read().decode()
        assert "fs_candles_received_total" in body
        assert "fs_polygon_ws_connected 1.0" in body
    finally:
        handle.stop()


def test_start_metrics_server_binds_to_loopback_only():
    registry.init_metrics(role="caching")
    port = _free_port()
    handle = server.start_metrics_server(port=port, host="127.0.0.1")
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(1)
            s.connect(("127.0.0.1", port))
    finally:
        handle.stop()


def test_start_metrics_server_without_init_raises():
    with pytest.raises(RuntimeError):
        server.start_metrics_server(port=_free_port())
