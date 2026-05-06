import logging
import threading
from http.server import HTTPServer

from prometheus_client import start_http_server

from fastscanner.pkg.observability.registry import scrape_registry

logger = logging.getLogger(__name__)


class MetricsServer:
    def __init__(self, server: HTTPServer, thread: threading.Thread):
        self._server = server
        self._thread = thread

    @property
    def port(self) -> int:
        return self._server.server_address[1]

    def stop(self) -> None:
        self._server.shutdown()
        self._thread.join(timeout=5)


def start_metrics_server(port: int, host: str = "127.0.0.1") -> MetricsServer:
    server, thread = start_http_server(
        port=port, addr=host, registry=scrape_registry()
    )
    logger.info("metrics: /metrics HTTP server listening on %s:%d", host, port)
    return MetricsServer(server, thread)
