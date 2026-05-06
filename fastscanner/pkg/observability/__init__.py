from fastscanner.pkg.observability import metrics
from fastscanner.pkg.observability.fastapi import (
    PrometheusMiddleware,
    mark_worker_dead,
    metrics_endpoint,
)
from fastscanner.pkg.observability.janitor import MetricsJanitor, reap_once
from fastscanner.pkg.observability.registry import (
    Role,
    get_role,
    init_metrics,
    is_multiproc,
    multiproc_dir,
    scrape_registry,
)
from fastscanner.pkg.observability.server import MetricsServer, start_metrics_server

__all__ = [
    "MetricsJanitor",
    "MetricsServer",
    "PrometheusMiddleware",
    "Role",
    "get_role",
    "init_metrics",
    "is_multiproc",
    "mark_worker_dead",
    "metrics",
    "metrics_endpoint",
    "multiproc_dir",
    "reap_once",
    "scrape_registry",
    "start_metrics_server",
]
