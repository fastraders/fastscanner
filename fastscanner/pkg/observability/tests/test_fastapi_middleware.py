import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
from prometheus_client import CollectorRegistry

from fastscanner.pkg.observability import metrics, registry
from fastscanner.pkg.observability.fastapi import (
    PrometheusMiddleware,
    metrics_endpoint,
)


def _read_sample(reg: CollectorRegistry, sample_name: str, **labels) -> float:
    for metric in reg.collect():
        for sample in metric.samples:
            if sample.name != sample_name:
                continue
            if all(sample.labels.get(k) == v for k, v in labels.items()):
                return sample.value
    raise AssertionError(f"sample {sample_name}{labels} not found")


def _build_app() -> FastAPI:
    app = FastAPI()
    app.add_middleware(PrometheusMiddleware)
    app.add_route("/metrics", metrics_endpoint, methods=["GET"])

    @app.get("/api/items/{item_id}")
    async def get_item(item_id: str):
        return {"id": item_id}

    @app.get("/api/boom")
    async def boom():
        raise HTTPException(status_code=503, detail="nope")

    @app.get("/api/crash")
    async def crash():
        raise RuntimeError("kaboom")

    return app


def test_middleware_records_route_template(fresh_registry: CollectorRegistry):
    registry.init_metrics(role="writer")
    metrics._reset_for_test(registry.scrape_registry())
    client = TestClient(_build_app())

    client.get("/api/items/AAPL")
    client.get("/api/items/MSFT")

    assert (
        _read_sample(
            registry.scrape_registry(),
            "fs_http_request_latency_seconds_count",
            route="/api/items/{item_id}",
            code="200",
        )
        == 2
    )


def test_middleware_records_status_code(fresh_registry: CollectorRegistry):
    registry.init_metrics(role="writer")
    metrics._reset_for_test(registry.scrape_registry())
    client = TestClient(_build_app())

    client.get("/api/boom")

    assert (
        _read_sample(
            registry.scrape_registry(),
            "fs_http_request_latency_seconds_count",
            route="/api/boom",
            code="503",
        )
        == 1
    )


def test_middleware_records_500_on_unhandled(fresh_registry: CollectorRegistry):
    registry.init_metrics(role="writer")
    metrics._reset_for_test(registry.scrape_registry())
    client = TestClient(_build_app(), raise_server_exceptions=False)

    client.get("/api/crash")

    assert (
        _read_sample(
            registry.scrape_registry(),
            "fs_http_request_latency_seconds_count",
            route="/api/crash",
            code="500",
        )
        == 1
    )


def test_middleware_skips_metrics_route(fresh_registry: CollectorRegistry):
    registry.init_metrics(role="writer")
    metrics._reset_for_test(registry.scrape_registry())
    client = TestClient(_build_app())

    response = client.get("/metrics")

    assert response.status_code == 200
    assert "fs_" in response.text
    with pytest.raises(AssertionError):
        _read_sample(
            registry.scrape_registry(),
            "fs_http_request_latency_seconds_count",
            route="/metrics",
            code="200",
        )


def test_middleware_records_unmatched_path(fresh_registry: CollectorRegistry):
    registry.init_metrics(role="writer")
    metrics._reset_for_test(registry.scrape_registry())
    client = TestClient(_build_app())

    client.get("/no/such/path")

    assert (
        _read_sample(
            registry.scrape_registry(),
            "fs_http_request_latency_seconds_count",
            route="__not_found__",
            code="404",
        )
        == 1
    )
