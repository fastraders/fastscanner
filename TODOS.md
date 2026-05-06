# TODOS

## Atomic max-update for slow indicator caches

**What:** Convert the read-modify-write running-max merge in
`NewsConfidenceIndicator._inline_fetch` (and the equivalent pattern in any
future ratchet-style slow indicator) to an atomic operation backed by a
Dragonfly Lua script or a `WATCH`/`MULTI`/`EXEC` CAS loop.

**Why:** Today only one producer process runs (`run_indicators_caching.py`),
so the read-modify-write is safe. If the caching service is ever scaled
horizontally, two producers can both read cache=70, compute new=80 and new=85
locally, and last-writer-wins lets the lower 80 overwrite 85. Lower scores
silently overwriting higher ones is exactly the bug the ratchet exists to
prevent.

**Context:** The `Cache` port in `fastscanner/services/indicators/ports.py`
exposes only `save(key, value)` and `get(key)`. There is no atomic `max`
operation. Dragonfly speaks Redis-compatible Lua scripts; the cleanest fix is
a small `MAX_INT_PAYLOAD` script that compares the JSON `value` field and
keeps the higher one. Alternative: optimistic concurrency via `WATCH`.

**Pros:** Future-proofs horizontal scale-out of the slow-indicators service.
Removes a silent footgun.

**Cons:** Adds Dragonfly-specific code paths (the indicator becomes coupled
to the cache backend rather than the generic `Cache` port). Adds tests that
exercise the script via a real Dragonfly instance, not a mock.

**Depends on / blocked by:** Nothing immediate. Only worth the work when
horizontal scale-out is on the roadmap.

**Captured by:** /plan-eng-review on 2026-05-05 (single-producer assumption
flagged by the Codex outside-voice review).

## Migrate observability alerting from Grafana to Alertmanager

**What:** Add a `prometheus-alertmanager` systemd unit; convert the
Grafana-provisioned alert YAML in `deploy/observability/grafana/provisioning/alerting/`
to Prometheus rule files under `deploy/observability/prometheus/rules/`; configure
Alertmanager routes/receivers (PagerDuty/Slack/email).

**Why:** The phase-1 observability rollout uses Grafana's built-in alerting because
it's one fewer systemd unit. Codex's outside-voice review flagged this as fragile:
if Grafana itself dies, alerts stop firing even when Prometheus is healthy.
Production-grade alerting separates evaluation (Alertmanager, fed by Prometheus)
from visualization (Grafana).

**Context:** Phase 1 alert YAML is provisioned via Grafana sidecar. The alert
rule expressions are PromQL, so the conversion to Prometheus rule files is
mostly mechanical. The new piece is Alertmanager configuration: routes,
receivers, inhibitors, and the contact-point integrations (currently sketched
in Grafana).

**Pros:** Alerts survive a Grafana outage. Industry-standard separation of
concerns. Better alert deduplication and inhibition than Grafana's built-in.

**Cons:** Adds 1 systemd unit. Notification routing has to be configured from
scratch. Two places to look at when an alert fires (Alertmanager UI for
suppression state, Grafana for the dashboard panel).

**Depends on / blocked by:** Phase-1 observability lands first.

**Captured by:** /plan-eng-review on 2026-05-06.

## Asyncio-native /metrics server for writer + caching

**What:** Replace `prometheus_client.start_http_server(port)` (threaded HTTP)
with an asyncio-native `/metrics` handler that runs in the same event loop as
the candle hot path, in `fastscanner/pkg/observability/server.py`.

**Why:** `start_http_server` runs the scrape handler in a background thread.
During each scrape, the thread serializes the entire registry — Python work
that contends for the GIL. For a sub-millisecond hot loop processing candle
messages, every scrape (~10-50ms of CPU work every 15s) preempts the loop
briefly. Codex's outside-voice review flagged this. Impact is small but real:
~0.07-0.3% extra latency, periodic.

**Context:** Affects `run_realtime_writer` and `run_indicators_caching` only —
the FastAPI process already serves `/metrics` natively via ASGI middleware
(no thread). An asyncio-native handler can use `aiohttp` (already a transitive
dep of nats-py) or stdlib `asyncio.start_server` with manual HTTP parsing.
Cooperative scheduling means the scrape never preempts a hot-loop iteration
mid-flight; it only runs when the loop yields.

**Pros:** Removes thread/GIL contention. Tail latency on the realtime hot
loop becomes measurably more predictable.

**Cons:** ~50-100 lines of HTTP server in our code instead of using the
library helper. One more thing to maintain.

**Depends on / blocked by:** Phase-1 observability lands first; only worth
doing if profiling or production observation suggests scrape-induced
latency variance matters.

**Captured by:** /plan-eng-review on 2026-05-06.

## NATSChannel.flush() timeout

**What:** Add a timeout wrapper around `NATSChannel.flush()` in
`fastscanner/adapters/realtime/nats_channel.py:47-56` so that a hung NATS
server stalling individual `nc.publish()` calls does not block the writer's
flush call indefinitely.

**Why:** Pre-existing bug surfaced by failure-mode analysis during the
phase-1 observability review (failure F3). The new `fs_nats_pending_messages`
gauge will reveal the *symptom* (queue depth climbs) but the cause is that
`flush()` itself can hang. With a timeout the writer can log + drop the
batch + reconnect and recover; without it, the realtime writer goes silent.

**Context:** Not introduced by the observability PR — present today. Probably
hasn't bitten because NATS is colocated and stable. Will become more relevant
once observability shows the symptom and operators expect recovery.

**Pros:** Writer recovers from NATS partial-stall instead of hanging
indefinitely. Bounded worst-case latency on flush.

**Cons:** Picking a timeout value is a tuning question (too short = false
recoveries during transient slowness; too long = hang persists). Reasonable
starting value: 1s for `flush()`, since we batch per WS message.

**Depends on / blocked by:** Nothing.

**Captured by:** /plan-eng-review on 2026-05-06 (failure-mode F3 analysis).
