from fastscanner.pkg.observability import metrics
from fastscanner.pkg.observability.fastapi import instrument_app
from fastscanner.pkg.observability.otel_init import Role
from fastscanner.pkg.observability.registry import (
    get_role,
    init_metrics,
    is_initialized,
)

__all__ = [
    "Role",
    "get_role",
    "init_metrics",
    "instrument_app",
    "is_initialized",
    "metrics",
]
