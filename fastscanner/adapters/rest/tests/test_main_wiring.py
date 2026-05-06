from fastapi.testclient import TestClient

from fastscanner.adapters.rest.main import app
from fastscanner.pkg.observability.fastapi import PrometheusMiddleware


def test_metrics_endpoint_is_mounted():
    routes = {getattr(r, "path", None) for r in app.routes}
    assert "/metrics" in routes


def test_prometheus_middleware_is_installed():
    middleware_classes = [m.cls for m in app.user_middleware]
    assert PrometheusMiddleware in middleware_classes


def test_metrics_endpoint_responds():
    client = TestClient(app)
    response = client.get("/metrics")
    assert response.status_code == 200
