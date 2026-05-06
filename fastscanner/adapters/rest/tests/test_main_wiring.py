from fastscanner.adapters.rest.main import app


def test_app_is_instrumented():
    # FastAPIInstrumentor.instrument_app marks the app with this attribute.
    assert getattr(app, "_is_instrumented_by_opentelemetry", False) is True


def test_app_has_no_metrics_route():
    # OTel migration: workers push via OTLP; no per-process /metrics endpoint.
    routes = {getattr(r, "path", None) for r in app.routes}
    assert "/metrics" not in routes
