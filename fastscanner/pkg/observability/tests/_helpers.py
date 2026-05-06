from typing import cast

from opentelemetry.sdk.metrics.export import (
    HistogramDataPoint,
    InMemoryMetricReader,
    NumberDataPoint,
)


def snapshot(reader: InMemoryMetricReader) -> dict[str, list]:
    """Collect once and bucket data points by metric name."""
    data = reader.get_metrics_data()
    out: dict[str, list] = {}
    if data is None:
        return out
    for resource_metrics in data.resource_metrics:
        for scope_metrics in resource_metrics.scope_metrics:
            for metric in scope_metrics.metrics:
                out.setdefault(metric.name, []).extend(metric.data.data_points)
    return out


def _find_raw(reader_or_snapshot, metric_name: str, **attrs):
    snap = (
        reader_or_snapshot
        if isinstance(reader_or_snapshot, dict)
        else snapshot(reader_or_snapshot)
    )
    points = snap.get(metric_name, [])
    for point in points:
        if all(point.attributes.get(k) == v for k, v in attrs.items()):
            return point
    raise AssertionError(
        f"data point {metric_name}{attrs} not found; "
        f"got names: {list(snap.keys())}; "
        f"points for name: {[(p.attributes,) for p in points]}"
    )


def find_point(reader_or_snapshot, metric_name: str, **attrs) -> NumberDataPoint:
    """Find a counter/gauge data point. Use find_histogram for histograms."""
    return cast(NumberDataPoint, _find_raw(reader_or_snapshot, metric_name, **attrs))


def find_histogram(
    reader_or_snapshot, metric_name: str, **attrs
) -> HistogramDataPoint:
    return cast(
        HistogramDataPoint, _find_raw(reader_or_snapshot, metric_name, **attrs)
    )


def read_value(reader_or_snapshot, metric_name: str, **attrs) -> float:
    return find_point(reader_or_snapshot, metric_name, **attrs).value
