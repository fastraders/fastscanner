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
