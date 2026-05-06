import logging
import os
import time

from prometheus_client import CONTENT_TYPE_LATEST, generate_latest, multiprocess
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response
from starlette.routing import Match

from fastscanner.pkg.observability import metrics
from fastscanner.pkg.observability.registry import is_multiproc, scrape_registry

logger = logging.getLogger(__name__)


def _route_template(request: Request) -> str:
    for route in request.app.routes:
        match, _ = route.matches(request.scope)
        if match == Match.FULL:
            return getattr(route, "path", request.url.path)
    return "__not_found__"


class PrometheusMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        if request.url.path == "/metrics":
            return await call_next(request)
        start = time.perf_counter()
        try:
            response: Response = await call_next(request)
        except Exception:
            elapsed = time.perf_counter() - start
            metrics.http_request(_route_template(request), 500, elapsed)
            raise
        elapsed = time.perf_counter() - start
        metrics.http_request(_route_template(request), response.status_code, elapsed)
        return response


async def metrics_endpoint(request: Request) -> Response:
    payload = generate_latest(scrape_registry())
    return Response(content=payload, media_type=CONTENT_TYPE_LATEST)


def mark_worker_dead() -> None:
    if not is_multiproc():
        return
    try:
        multiprocess.mark_process_dead(os.getpid())
    except Exception:
        logger.exception("metrics: mark_process_dead failed for pid=%d", os.getpid())
