import logging
import os
from typing import Literal

from opentelemetry import metrics as otel_metrics
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.metrics import _internal as _otel_metrics_internal  # type: ignore[attr-defined]
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import (
    InMemoryMetricReader,
    PeriodicExportingMetricReader,
)
from opentelemetry.sdk.resources import Resource

logger = logging.getLogger(__name__)

Role = Literal["rest_api", "writer", "caching", "test"]

_OTLP_ENDPOINT_ENV = "OTEL_EXPORTER_OTLP_ENDPOINT"
_DEFAULT_OTLP_ENDPOINT = "http://localhost:4317"
_DEFAULT_EXPORT_INTERVAL_MS = 10_000


class _ProviderState:
    provider: MeterProvider | None = None
    in_memory_reader: InMemoryMetricReader | None = None
    role: Role | None = None


_state = _ProviderState()


def setup_meter_provider(role: Role, *, in_memory: bool = False) -> MeterProvider:
    if _state.provider is not None:
        if _state.role != role:
            raise RuntimeError(
                f"setup_meter_provider already called with role={_state.role!r}; "
                f"cannot re-initialize with role={role!r}"
            )
        return _state.provider

    resource = Resource.create({"service.name": "fastscanner", "service.role": role})

    if in_memory:
        reader: PeriodicExportingMetricReader | InMemoryMetricReader = (
            InMemoryMetricReader()
        )
        _state.in_memory_reader = reader  # type: ignore[assignment]
    else:
        endpoint = os.environ.get(_OTLP_ENDPOINT_ENV, _DEFAULT_OTLP_ENDPOINT)
        exporter = OTLPMetricExporter(endpoint=endpoint, insecure=True)
        reader = PeriodicExportingMetricReader(
            exporter, export_interval_millis=_DEFAULT_EXPORT_INTERVAL_MS
        )

    provider = MeterProvider(resource=resource, metric_readers=[reader])
    otel_metrics.set_meter_provider(provider)
    _state.provider = provider
    _state.role = role
    logger.info("otel: MeterProvider initialized (role=%s, in_memory=%s)", role, in_memory)
    return provider


def in_memory_reader() -> InMemoryMetricReader:
    if _state.in_memory_reader is None:
        raise RuntimeError(
            "in_memory_reader requested but setup_meter_provider was not called "
            "with in_memory=True"
        )
    return _state.in_memory_reader


def get_role() -> Role:
    if _state.role is None:
        raise RuntimeError("setup_meter_provider has not been called")
    return _state.role


def is_initialized() -> bool:
    return _state.provider is not None


def shutdown() -> None:
    if _state.provider is not None:
        _state.provider.shutdown()
    _state.provider = None
    _state.in_memory_reader = None
    _state.role = None
    # OTel guards `set_meter_provider` with a one-shot Once. Tests need to
    # rebuild the provider; reach into the internal flag to allow it.
    _m = _otel_metrics_internal
    _m._METER_PROVIDER_SET_ONCE = type(_m._METER_PROVIDER_SET_ONCE)()
    _m._METER_PROVIDER = None
    _m._PROXY_METER_PROVIDER = _m._ProxyMeterProvider()
