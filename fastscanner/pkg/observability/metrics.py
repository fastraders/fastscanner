from typing import Literal

from opentelemetry.metrics import (
    Counter,
    Histogram,
    UpDownCounter,
    _Gauge,
    get_meter,
)

from fastscanner.pkg.observability import buckets, otel_init

CacheSaveResult = Literal["ok", "error"]
SubscriptionKind = Literal[
    "websocket_indicator",
    "scanner_wildcard",
    "indicator_fanout",
]
RequestSource = Literal["polygon_rest", "eodhd", "news", "internal"]
WSEndpoint = Literal["indicator", "scanner"]


class _Metrics:
    def __init__(self) -> None:
        meter = get_meter("fastscanner")

        self.candles_received_total: Counter = meter.create_counter(
            name="fs.candles.received",
            description="Candles reconstructed from the realtime feed.",
        )
        self.nats_publish_total: Counter = meter.create_counter(
            name="fs.nats.publish",
            description="NATS messages published, by subject prefix kind.",
        )
        self.nats_publish_errors_total: Counter = meter.create_counter(
            name="fs.nats.publish.errors",
            description="NATS publish exceptions, by subject prefix kind.",
        )
        self.nats_reconnect_total: Counter = meter.create_counter(
            name="fs.nats.reconnect",
            description="NATS client reconnections.",
        )
        self.indicator_cache_save_total: Counter = meter.create_counter(
            name="fs.indicator.cache.save",
            description="Indicator cache save attempts.",
        )
        self.indicator_extend_errors_total: Counter = meter.create_counter(
            name="fs.indicator.extend.errors",
            description="Indicator extend_realtime exceptions, by indicator name.",
        )
        self.external_requests_total: Counter = meter.create_counter(
            name="fs.external.requests",
            description="Outbound requests by source, endpoint template, and status code.",
        )
        self.external_request_errors_total: Counter = meter.create_counter(
            name="fs.external.request.errors",
            description="Outbound request exceptions by source, endpoint template, and kind.",
        )

        self.nats_flush_duration_seconds: Histogram = meter.create_histogram(
            name="fs.nats.flush.duration",
            unit="s",
            description="Latency of NATSChannel.flush(), one observation per flush call.",
            explicit_bucket_boundaries_advisory=list(buckets.NATS_FLUSH_SECONDS[:-1]),
        )
        self.indicator_extend_latency_seconds: Histogram = meter.create_histogram(
            name="fs.indicator.extend.latency",
            unit="s",
            description="Latency of indicator.extend_realtime() per indicator name.",
            explicit_bucket_boundaries_advisory=list(
                buckets.INDICATOR_EXTEND_SECONDS[:-1]
            ),
        )
        self.external_request_latency_seconds: Histogram = meter.create_histogram(
            name="fs.external.request.latency",
            unit="s",
            description="Outbound request latency by source and endpoint template.",
            explicit_bucket_boundaries_advisory=list(buckets.HTTP_REQUEST_SECONDS[:-1]),
        )
        self.dragonfly_command_latency_seconds: Histogram = meter.create_histogram(
            name="fs.dragonfly.command.latency",
            unit="s",
            description="Dragonfly command latency by op and outcome.",
            explicit_bucket_boundaries_advisory=list(
                buckets.DRAGONFLY_COMMAND_SECONDS[:-1]
            ),
        )

        self.ws_connections_active: UpDownCounter = meter.create_up_down_counter(
            name="fs.ws.connections.active",
            description="Active websocket connections by endpoint.",
        )
        self.ws_connections_opened_total: Counter = meter.create_counter(
            name="fs.ws.connections.opened",
            description="Websocket connections opened, by endpoint.",
        )
        self.ws_connections_closed_total: Counter = meter.create_counter(
            name="fs.ws.connections.closed",
            description="Websocket connections closed, by endpoint and close reason.",
        )
        self.ws_subscribe_errors_total: Counter = meter.create_counter(
            name="fs.ws.subscribe.errors",
            description="Websocket subscribe failures, by endpoint and kind.",
        )
        self.ws_messages_pushed_total: Counter = meter.create_counter(
            name="fs.ws.message.pushed",
            description="Websocket messages pushed to clients, by endpoint and result.",
        )
        self.ws_scanner_match_emitted_total: Counter = meter.create_counter(
            name="fs.ws.scanner.match.emitted",
            description="Scanner websocket match messages emitted, by scanner_id and freq.",
        )
        self.ws_subscribe_latency_seconds: Histogram = meter.create_histogram(
            name="fs.ws.subscribe.latency",
            unit="s",
            description="Websocket subscribe handler latency, by endpoint and result.",
            explicit_bucket_boundaries_advisory=list(
                buckets.WS_SUBSCRIBE_SECONDS[:-1]
            ),
        )
        self.ws_message_push_latency_seconds: Histogram = meter.create_histogram(
            name="fs.ws.message.push.latency",
            unit="s",
            description="Time spent inside websocket.send_text(), by endpoint.",
            explicit_bucket_boundaries_advisory=list(buckets.WS_PUSH_SECONDS[:-1]),
        )
        self.ws_candle_to_client_latency_seconds: Histogram = meter.create_histogram(
            name="fs.ws.candle.to.client.latency",
            unit="s",
            description=(
                "Wall-clock seconds past bar end when the bar reached the websocket "
                "client, by endpoint and freq."
            ),
            explicit_bucket_boundaries_advisory=list(
                buckets.CANDLE_TO_CLIENT_SECONDS[:-1]
            ),
        )

        self.daily_symbols_processed_total: Counter = meter.create_counter(
            name="fs.daily.symbols_processed",
            description="Symbols completed by the daily collection job, by outcome.",
        )
        self.daily_symbol_failures_total: Counter = meter.create_counter(
            name="fs.daily.symbol.failures",
            description="Daily collection per-symbol failures, by exception kind.",
        )
        self.daily_symbol_duration_seconds: Histogram = meter.create_histogram(
            name="fs.daily.symbol.duration",
            unit="s",
            description="Per-symbol wall-clock duration of daily collection.",
            explicit_bucket_boundaries_advisory=list(
                buckets.SYMBOL_DURATION_SECONDS[:-1]
            ),
        )
        self.daily_symbols_total: _Gauge = meter.create_gauge(
            name="fs.daily.symbols.total",
            description="Total symbols this worker was given to process.",
        )
        self.daily_symbols_remaining: _Gauge = meter.create_gauge(
            name="fs.daily.symbols.remaining",
            description="Symbols this worker has left to process.",
        )
        self.daily_worker_active: _Gauge = meter.create_gauge(
            name="fs.daily.worker.active",
            description="1 while a daily collection worker is running, 0 otherwise.",
        )

        self.candle_partition_lookup_total: Counter = meter.create_counter(
            name="fs.candle.partition.cache_lookup",
            description="Partitioned-CSV cache lookup outcomes, by result and freq.",
        )
        self.candle_partition_write_total: Counter = meter.create_counter(
            name="fs.candle.partition.write",
            description="Partitioned-CSV writes, by freq and outcome (written|empty).",
        )
        self.candle_partition_write_duration_seconds: Histogram = meter.create_histogram(
            name="fs.candle.partition.write.duration",
            unit="s",
            description="Time spent in pandas to_csv() for a partition write.",
            explicit_bucket_boundaries_advisory=list(
                buckets.PARTITION_WRITE_SECONDS[:-1]
            ),
        )

        self.ratelimit_wait_seconds: Histogram = meter.create_histogram(
            name="fs.ratelimit.wait",
            unit="s",
            description="Async rate-limiter wait time, by limiter name.",
            explicit_bucket_boundaries_advisory=list(
                buckets.RATELIMIT_WAIT_SECONDS[:-1]
            ),
        )

        self.indicator_cache_save_duration_seconds: Histogram = meter.create_histogram(
            name="fs.indicator.cache.save.duration",
            unit="s",
            description="save_to_cache() duration per indicator name.",
            explicit_bucket_boundaries_advisory=list(buckets.CACHE_SAVE_SECONDS[:-1]),
        )
        self.indicator_cache_last_success_ts: _Gauge = meter.create_gauge(
            name="fs.indicator.cache.last.success.timestamp",
            description="Unix epoch seconds of the last successful cache save per indicator.",
        )
        self.indicator_caching_loop_lag_seconds: _Gauge = meter.create_gauge(
            name="fs.indicator.caching.loop.lag",
            unit="s",
            description="Caching loop scheduling lag: actual wake - scheduled wake.",
        )

        self.indicator_slow_fetch_duration_seconds: Histogram = meter.create_histogram(
            name="fs.indicator.slow.fetch.duration",
            unit="s",
            description="Duration of slow-indicator inline fetch, by indicator and outcome.",
            explicit_bucket_boundaries_advisory=list(buckets.SLOW_FETCH_SECONDS[:-1]),
        )
        self.indicator_slow_inflight: _Gauge = meter.create_gauge(
            name="fs.indicator.slow.inflight",
            description="In-flight slow-indicator background fetches, by indicator type.",
        )

        self.codex_invocations_total: Counter = meter.create_counter(
            name="fs.codex.invocations",
            description="Codex CLI invocations from the news pipeline, by outcome.",
        )
        self.codex_duration_seconds: Histogram = meter.create_histogram(
            name="fs.codex.duration",
            unit="s",
            description="Codex CLI wall time, by outcome.",
            explicit_bucket_boundaries_advisory=list(buckets.SLOW_FETCH_SECONDS[:-1]),
        )

        self.news_first_to_publish_total: Counter = meter.create_counter(
            name="fs.news.first_to_publish",
            description=(
                "New (first-time-seen today) headlines scored above a "
                "confidence threshold, attributed by source provider."
            ),
        )

        self.first_candle_delay_seconds: _Gauge = meter.create_gauge(
            name="fs.first.candle.delay",
            unit="s",
            description=(
                "Wall-clock seconds past the bar end when the FIRST candle for the "
                "most recently observed minute arrived from Polygon."
            ),
        )
        self.candle_arrival_spread_seconds: _Gauge = meter.create_gauge(
            name="fs.candle.arrival.spread",
            unit="s",
            description=(
                "Seconds between the first and last candle arrival for the most "
                "recently completed minute bar."
            ),
        )
        self.nats_pending_messages: _Gauge = meter.create_gauge(
            name="fs.nats.pending.messages",
            description="Depth of NATSChannel._pending_messages at flush boundary.",
        )
        self.polygon_ws_connected: _Gauge = meter.create_gauge(
            name="fs.polygon.ws.connected",
            description="1 if the Polygon WebSocket is connected; 0 otherwise.",
        )
        self.active_subscriptions: _Gauge = meter.create_gauge(
            name="fs.active.subscriptions",
            description="Active subscription count by kind.",
        )
        self.market_is_open: _Gauge = meter.create_gauge(
            name="fs.market.is.open",
            description="1 if the equity market is currently open; 0 otherwise.",
        )
        self.daily_reset_in_progress: _Gauge = meter.create_gauge(
            name="fs.daily.reset.in.progress",
            description="1 while the realtime writer is performing scheduled daily reset.",
        )

    def candle_received(self, symbol_class: str) -> None:
        self.candles_received_total.add(1, {"symbol_class": symbol_class})

    def nats_publish(self, kind: str) -> None:
        self.nats_publish_total.add(1, {"kind": kind})

    def nats_publish_error(self, kind: str) -> None:
        self.nats_publish_errors_total.add(1, {"kind": kind})

    def nats_reconnect(self) -> None:
        self.nats_reconnect_total.add(1)

    def nats_flush(self, latency_seconds: float) -> None:
        self.nats_flush_duration_seconds.record(latency_seconds)

    def nats_pending(self, depth: int) -> None:
        self.nats_pending_messages.set(depth)

    def indicator_extend_latency(self, name: str, latency_seconds: float) -> None:
        self.indicator_extend_latency_seconds.record(
            latency_seconds, {"name": name}
        )

    def indicator_extend_error(self, name: str) -> None:
        self.indicator_extend_errors_total.add(1, {"name": name})

    def indicator_cache_save(self, name: str, result: CacheSaveResult) -> None:
        self.indicator_cache_save_total.add(1, {"name": name, "result": result})

    def external_request(
        self, source: str, endpoint: str, code: int, latency_seconds: float
    ) -> None:
        attrs = {"source": source, "endpoint": endpoint, "code": str(code)}
        self.external_requests_total.add(1, attrs)
        self.external_request_latency_seconds.record(
            latency_seconds, {"source": source, "endpoint": endpoint}
        )

    def external_request_error(self, source: str, endpoint: str, kind: str) -> None:
        self.external_request_errors_total.add(
            1, {"source": source, "endpoint": endpoint, "kind": kind}
        )

    def dragonfly_command(
        self, op: str, outcome: str, latency_seconds: float
    ) -> None:
        self.dragonfly_command_latency_seconds.record(
            latency_seconds, {"op": op, "outcome": outcome}
        )

    def ws_connection_opened(self, endpoint: str) -> None:
        self.ws_connections_opened_total.add(1, {"endpoint": endpoint})
        self.ws_connections_active.add(1, {"endpoint": endpoint})

    def ws_connection_closed(self, endpoint: str, reason: str) -> None:
        self.ws_connections_closed_total.add(
            1, {"endpoint": endpoint, "reason": reason}
        )
        self.ws_connections_active.add(-1, {"endpoint": endpoint})

    def ws_subscribe_latency(
        self, endpoint: str, result: str, latency_seconds: float
    ) -> None:
        self.ws_subscribe_latency_seconds.record(
            latency_seconds, {"endpoint": endpoint, "result": result}
        )

    def ws_subscribe_error(self, endpoint: str, kind: str) -> None:
        self.ws_subscribe_errors_total.add(1, {"endpoint": endpoint, "kind": kind})

    def ws_message_pushed(
        self, endpoint: str, result: str, latency_seconds: float
    ) -> None:
        self.ws_messages_pushed_total.add(
            1, {"endpoint": endpoint, "result": result}
        )
        if result == "ok":
            self.ws_message_push_latency_seconds.record(
                latency_seconds, {"endpoint": endpoint}
            )

    def ws_candle_to_client(
        self, endpoint: str, freq: str, latency_seconds: float
    ) -> None:
        self.ws_candle_to_client_latency_seconds.record(
            latency_seconds, {"endpoint": endpoint, "freq": freq}
        )

    def ws_scanner_match(self, scanner_id: str, freq: str) -> None:
        self.ws_scanner_match_emitted_total.add(
            1, {"scanner_id": scanner_id, "freq": freq}
        )

    def daily_symbol_processed(
        self, outcome: str, duration_seconds: float
    ) -> None:
        self.daily_symbols_processed_total.add(1, {"outcome": outcome})
        self.daily_symbol_duration_seconds.record(
            duration_seconds, {"outcome": outcome}
        )

    def daily_symbol_failure(self, kind: str) -> None:
        self.daily_symbol_failures_total.add(1, {"kind": kind})

    def daily_set_progress(self, total: int, remaining: int) -> None:
        self.daily_symbols_total.set(total)
        self.daily_symbols_remaining.set(remaining)

    def daily_set_worker_active(self, active: bool) -> None:
        self.daily_worker_active.set(1 if active else 0)

    def candle_partition_lookup(self, result: str, freq: str) -> None:
        self.candle_partition_lookup_total.add(1, {"result": result, "freq": freq})

    def candle_partition_write(
        self, freq: str, outcome: str, duration_seconds: float
    ) -> None:
        self.candle_partition_write_total.add(1, {"freq": freq, "outcome": outcome})
        self.candle_partition_write_duration_seconds.record(
            duration_seconds, {"freq": freq}
        )

    def ratelimit_wait(self, name: str, wait_seconds: float) -> None:
        self.ratelimit_wait_seconds.record(wait_seconds, {"name": name})

    def indicator_cache_save_duration(
        self, name: str, duration_seconds: float
    ) -> None:
        self.indicator_cache_save_duration_seconds.record(
            duration_seconds, {"name": name}
        )

    def indicator_cache_last_success(self, name: str, ts_seconds: float) -> None:
        self.indicator_cache_last_success_ts.set(ts_seconds, {"name": name})

    def indicator_caching_loop_lag(self, lag_seconds: float) -> None:
        self.indicator_caching_loop_lag_seconds.set(lag_seconds)

    def indicator_slow_fetch(
        self, indicator: str, outcome: str, duration_seconds: float
    ) -> None:
        self.indicator_slow_fetch_duration_seconds.record(
            duration_seconds, {"indicator": indicator, "outcome": outcome}
        )

    def indicator_slow_inflight_set(self, indicator: str, count: int) -> None:
        self.indicator_slow_inflight.set(count, {"indicator": indicator})

    def codex_invocation(self, outcome: str, duration_seconds: float) -> None:
        self.codex_invocations_total.add(1, {"outcome": outcome})
        self.codex_duration_seconds.record(duration_seconds, {"outcome": outcome})

    def news_first_to_publish(self, source: str, threshold: int) -> None:
        self.news_first_to_publish_total.add(
            1, {"source": source, "threshold": str(threshold)}
        )

    def set_first_candle_delay(self, delay_seconds: float) -> None:
        self.first_candle_delay_seconds.set(delay_seconds)

    def set_candle_arrival_spread(self, spread_seconds: float) -> None:
        self.candle_arrival_spread_seconds.set(spread_seconds)

    def set_ws_connected(self, connected: bool) -> None:
        self.polygon_ws_connected.set(1 if connected else 0)

    def set_active_subscriptions(self, kind: SubscriptionKind, count: int) -> None:
        self.active_subscriptions.set(count, {"kind": kind})

    def set_market_open(self, is_open: bool) -> None:
        self.market_is_open.set(1 if is_open else 0)

    def set_daily_reset(self, in_progress: bool) -> None:
        self.daily_reset_in_progress.set(1 if in_progress else 0)


_instance: _Metrics | None = None


def _get() -> _Metrics:
    global _instance
    if _instance is None:
        if not otel_init.is_initialized():
            raise RuntimeError(
                "metrics helper called before otel_init.setup_meter_provider(). "
                "Each long-running process must call init_metrics(role=...) at startup."
            )
        _instance = _Metrics()
    return _instance


def candle_received(symbol_class: str) -> None:
    _get().candle_received(symbol_class)


def nats_publish(kind: str) -> None:
    _get().nats_publish(kind)


def nats_publish_error(kind: str) -> None:
    _get().nats_publish_error(kind)


def nats_reconnect() -> None:
    _get().nats_reconnect()


def nats_flush(latency_seconds: float) -> None:
    _get().nats_flush(latency_seconds)


def nats_pending(depth: int) -> None:
    _get().nats_pending(depth)


def indicator_extend_latency(name: str, latency_seconds: float) -> None:
    _get().indicator_extend_latency(name, latency_seconds)


def indicator_extend_error(name: str) -> None:
    _get().indicator_extend_error(name)


def indicator_cache_save(name: str, result: CacheSaveResult) -> None:
    _get().indicator_cache_save(name, result)


def external_request(
    source: str, endpoint: str, code: int, latency_seconds: float
) -> None:
    _get().external_request(source, endpoint, code, latency_seconds)


def external_request_error(source: str, endpoint: str, kind: str) -> None:
    _get().external_request_error(source, endpoint, kind)


def dragonfly_command(op: str, outcome: str, latency_seconds: float) -> None:
    _get().dragonfly_command(op, outcome, latency_seconds)


def ws_connection_opened(endpoint: str) -> None:
    _get().ws_connection_opened(endpoint)


def ws_connection_closed(endpoint: str, reason: str) -> None:
    _get().ws_connection_closed(endpoint, reason)


def ws_subscribe_latency(
    endpoint: str, result: str, latency_seconds: float
) -> None:
    _get().ws_subscribe_latency(endpoint, result, latency_seconds)


def ws_subscribe_error(endpoint: str, kind: str) -> None:
    _get().ws_subscribe_error(endpoint, kind)


def ws_message_pushed(endpoint: str, result: str, latency_seconds: float) -> None:
    _get().ws_message_pushed(endpoint, result, latency_seconds)


def ws_candle_to_client(endpoint: str, freq: str, latency_seconds: float) -> None:
    _get().ws_candle_to_client(endpoint, freq, latency_seconds)


def ws_scanner_match(scanner_id: str, freq: str) -> None:
    _get().ws_scanner_match(scanner_id, freq)


def daily_symbol_processed(outcome: str, duration_seconds: float) -> None:
    _get().daily_symbol_processed(outcome, duration_seconds)


def daily_symbol_failure(kind: str) -> None:
    _get().daily_symbol_failure(kind)


def daily_set_progress(total: int, remaining: int) -> None:
    _get().daily_set_progress(total, remaining)


def daily_set_worker_active(active: bool) -> None:
    _get().daily_set_worker_active(active)


def candle_partition_lookup(result: str, freq: str) -> None:
    _get().candle_partition_lookup(result, freq)


def candle_partition_write(freq: str, outcome: str, duration_seconds: float) -> None:
    _get().candle_partition_write(freq, outcome, duration_seconds)


def ratelimit_wait(name: str, wait_seconds: float) -> None:
    _get().ratelimit_wait(name, wait_seconds)


def indicator_cache_save_duration(name: str, duration_seconds: float) -> None:
    _get().indicator_cache_save_duration(name, duration_seconds)


def indicator_cache_last_success(name: str, ts_seconds: float) -> None:
    _get().indicator_cache_last_success(name, ts_seconds)


def indicator_caching_loop_lag(lag_seconds: float) -> None:
    _get().indicator_caching_loop_lag(lag_seconds)


def indicator_slow_fetch(
    indicator: str, outcome: str, duration_seconds: float
) -> None:
    _get().indicator_slow_fetch(indicator, outcome, duration_seconds)


def indicator_slow_inflight_set(indicator: str, count: int) -> None:
    _get().indicator_slow_inflight_set(indicator, count)


def codex_invocation(outcome: str, duration_seconds: float) -> None:
    _get().codex_invocation(outcome, duration_seconds)


def news_first_to_publish(source: str, threshold: int) -> None:
    _get().news_first_to_publish(source, threshold)


def set_first_candle_delay(delay_seconds: float) -> None:
    _get().set_first_candle_delay(delay_seconds)


def set_candle_arrival_spread(spread_seconds: float) -> None:
    _get().set_candle_arrival_spread(spread_seconds)


def set_ws_connected(connected: bool) -> None:
    _get().set_ws_connected(connected)


def set_active_subscriptions(kind: SubscriptionKind, count: int) -> None:
    _get().set_active_subscriptions(kind, count)


def set_market_open(is_open: bool) -> None:
    _get().set_market_open(is_open)


def set_daily_reset(in_progress: bool) -> None:
    _get().set_daily_reset(in_progress)


def _reset_for_test() -> None:
    global _instance
    _instance = None
