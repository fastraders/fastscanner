# Phase 2: Security & Performance Review

## Security Findings

> **Context**: All endpoints run in a private network, serving only internal applications developed by the same team. Trusted callers only. Fail-fast on bad input is the correct approach.

### High

1. **Exception details leaked to WebSocket clients** (CWE-209)
   - Raw Python exception messages sent on subscription failure, exposing internal paths/state to calling services. Even in an internal context, this makes debugging harder on the client side (noisy raw tracebacks vs actionable messages).
   - **Fix**: Return generic error messages; log details server-side only.

### Medium

2. **Cache deserialization without validation** (CWE-502)
   - `json.loads()` on cache data used directly in financial calculations with no type validation. Malformed values (e.g., strings where floats expected) propagate silently and corrupt indicator outputs rather than failing fast.
   - **Fix**: Add type assertions (`float(v)`) after deserialization to fail fast on bad data.

3. **WebSocket subscription ID collision** (CWE-639)
   - Client-controlled `subscription_id` allows overwriting another service's subscription by reusing the same ID (the old dict entry is silently replaced). A service can also craft an ID starting with `"persister_"` to bypass event publishing logic.
   - **Fix**: Generate IDs server-side; validate reserved prefixes.

4. **Global mutable state in ApplicationRegistry** (CWE-362)
   - Class-level global service locator with mutable state. Multiprocessing workers get forked copies with potentially stale financial data across workers.
   - **Fix**: Replace with dependency injection or `ContextVar`-based scoping.

5. **Unvalidated `freq` parameter** (CWE-20)
   - Free-form string passed to channel key construction. Invalid units cause `KeyError` at runtime with no clear error message — violates fail-fast principle.
   - **Fix**: Validate with enum or allowlist for a clear error on bad input.

### Low

6. **Raw WebSocket data logged on error** (CWE-532) — truncate logged data to prevent log bloat from malformed messages.
7. **`logging.basicConfig` at module level** — can override application logging configuration.
8. **Debug mode without production safeguards** — `DEBUG` env var enables uvicorn reload with no environment guard.

---

## Performance Findings

### Critical

1. **DailyIndicatorCache thundering herd on day boundary**
   - Each subscription creates its own `DailyIndicatorCache` instance. On first candle of new day, (N+M) subscriptions each reload ALL indicator columns sequentially from cache. With many subscriptions, this causes a seconds-long latency spike at market open.
   - **Fix**: Promote to shared singleton with `asyncio.Lock` for single reload.

### High

2. **Sequential cache loading in `load_all_cacheable`**
   - Scanner subscription setup awaits each indicator's `load_from_cache()` sequentially (7+ cache reads). Could run concurrently.
   - **Fix**: Use `asyncio.gather` for concurrent cache loads.

3. **Unbounded `_buffers` dict growth in channel handlers**
   - `_buffers: dict[str, CandleBuffer]` grows with every new symbol but entries are never removed. Slow memory leak proportional to total unique symbols ever seen.
   - **Fix**: Implement periodic cleanup of stale buffers.

4. **DataFrame construction on realtime hot path**
   - `_row_to_dataframe` creates a full `pd.DataFrame` from a single row dict. Called on cold-start for several indicators across thousands of symbols.
   - **Fix**: Compute initial values directly from row dict instead of DataFrame roundtrip.

5. **Sequential symbol iteration in daily caching script**
   - O(indicators * symbols) sequential async calls (~45,000 for 9 indicators x 5000 symbols).
   - **Fix**: Use `asyncio.Semaphore` with `asyncio.gather` for bounded parallelism.

### Medium

6. **DailyRollingIndicator fetches history on every day boundary per symbol** — burst of I/O. Pre-compute during daily caching.
7. **Scanner code duplication with redundant indicator instances** — each scanner creates its own indicator set; sharing across scanners would save memory.
8. **`pd.to_datetime` on every incoming message** — ~20-50us per call across thousands of symbols. Use `datetime.fromtimestamp` with `zoneinfo` on hot path.
9. **`model_dump_json` on every WebSocket message** — ~50-200us per message. Use `orjson.dumps` directly.
10. **Full JSON deserialization of all-symbol maps on cache load** — acceptable if shared cache (finding #1) is fixed.
11. **`CandleBuffer` timeout sleep can go negative** — clamp with `max(0, ...)`.
12. **`multiprocessing.Pool` recreated per `scan_all` request** — 2-5s process spawn overhead. Maintain persistent pool.
13. **Missing error handling in `load_from_cache` methods** — unhandled exception on corrupt cache kills subscription setup.
14. **Indicator state dicts grow without bound** — never trimmed for delisted symbols.

### Low

15. **`ShiftIndicator` uses `list.pop(0)` instead of `deque`** — O(n) vs O(1) popleft.
16. **Benchmark uses global mutable state** — not production code, but would break under concurrency.

---

## Critical Issues for Phase 3 Context

- **Cache format mismatch** (Phase 1) + **cache deserialization without validation** (Security M-2) together create a data integrity risk needing test coverage.
- **SmallCapUpScanner loop bug** (Phase 1 Critical) is a correctness defect needing a regression test.
- **`CandleStoreTest` undefined variables** (Phase 1 Critical) means existing scanner tests may not be running correctly.
- **Thundering herd** (Perf Critical) and **sequential cache loading** (Perf High) need performance test coverage.
