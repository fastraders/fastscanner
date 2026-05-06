import logging

from fastscanner.pkg.observability import metrics, otel_init
from fastscanner.pkg.observability.otel_init import Role

logger = logging.getLogger(__name__)


def init_metrics(role: Role, *, in_memory: bool = False) -> None:
    otel_init.setup_meter_provider(role, in_memory=in_memory)
    logger.info("metrics: initialized for role=%s", role)


def get_role() -> Role:
    return otel_init.get_role()


def is_initialized() -> bool:
    return otel_init.is_initialized()


def _reset_for_test() -> None:
    otel_init.shutdown()
    metrics._reset_for_test()
