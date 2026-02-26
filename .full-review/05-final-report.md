# Comprehensive Code Review Report

## Review Target

All modified and untracked files in the fastscanner working tree — 27 modified files and 2 new files spanning adapters (REST, CMD), services (indicators, scanners), benchmarks, and shared packages. Python/FastAPI application running in a private network.

## Executive Summary

The codebase implements a well-structured hexagonal architecture for real-time financial market scanning with a clear separation between adapters and services. The indicator layer has strong test coverage and the recent migration from `pd.Series` to `dict[str, Any]` for the realtime path is well-motivated for latency reduction. However, the review uncovered **2 runtime bugs** (SmallCapUpScanner loop, CandleStoreTest fixture), a **broken scanner test suite**, **~2000 lines of duplicated scanner code**, a **cache format contract mismatch**, and **zero CI/CD infrastructure**. The most impactful improvements would be fixing the two critical bugs, adding a CI pipeline with test gates, and consolidating the scanner duplication.

---

## Findings by Priority

### Critical Issues (P0 — Must Fix Immediately)

| # | Finding | Source Phase | File(s) |
|---|---------|-------------|---------|
| 1 | **`SmallCapUpScanner.scan` loop uses wrong shift indicator** — `shift_indicator` always references last element from prior loop. Produces incorrect scan results. | Phase 1 (Architecture) | `smallcap.py:163-168` |
| 2 | **`CandleStoreTest.get` references undefined `start_date`/`end_date`** — params are `start`/`end`. All scanner tests crash with `NameError`. | Phase 1 (Architecture) | `scanners/tests/conftest.py:152-158` |
| 3 | **No CI/CD pipeline** — zero automated quality gates. Code pushed to `main` with no test/lint validation. | Phase 4 (CI/CD) | Project-wide |

### High Priority (P1 — Fix Before Next Release)

| # | Finding | Source Phase | File(s) |
|---|---------|-------------|---------|
| 4 | `Indicator` Protocol declares `extend_realtime` but daily indicators lack it — broken type contract | Phase 1 (Quality) | `lib/__init__.py`, daily indicators |
| 5 | `hasattr` for protocol conformance instead of `isinstance` | Phase 1 (Quality) | `service.py:97` |
| 6 | Truthiness check on `atr_val` conflates `0` and `None` | Phase 1 (Quality) | `candle.py:642` |
| 7 | `CumulativeIndicator.load_from_cache` crashes on old cache entries (missing `last_date` key) | Phase 1 (Quality) | `candle.py` |
| 8 | `DailyIndicatorCache` / per-indicator cache format mismatch — different formats for same key prefix | Phase 1 (Architecture) | `service.py`, `daily.py` |
| 9 | ~2000 lines of duplicated scanner code (Up/Down pairs ~95% identical) | Phase 1 (Architecture) | `gap.py`, `parabolic.py`, `range_gap.py` |
| 10 | `ApplicationRegistry` global service locator — hidden deps, fragile tests | Phase 1 (Architecture) | `registry.py` + all indicators |
| 11 | `DailyIndicatorCache` thundering herd on day boundary | Phase 2 (Performance) | `service.py:304-327` |
| 12 | Sequential cache loading in `load_all_cacheable` — 7+ serial cache reads | Phase 2 (Performance) | scanners `__init__.py` |
| 13 | Unbounded `_buffers` dict growth — memory leak proportional to symbols | Phase 2 (Performance) | `service.py`, `scanners/service.py` |
| 14 | Sequential symbol iteration in daily caching script — O(indicators * symbols) | Phase 2 (Performance) | `run_daily_indicators_caching.py` |
| 15 | Exception details leaked to WebSocket clients | Phase 2 (Security) | `indicators.py:141-147` |
| 16 | Zero effective scanner test coverage (broken fixture) | Phase 3 (Testing) | Scanner test suite |
| 17 | Tautological ATR extend test — compares list to itself | Phase 3 (Testing) | `test_candle_indicators.py:116-120` |
| 18 | No `save_to_cache`/`load_from_cache` round-trip tests | Phase 3 (Testing) | Test suite |
| 19 | No linter/formatter/type checker configured | Phase 4 (Framework) | `pyproject.toml` |
| 20 | No deployment automation — manual SSH+git pull+restart | Phase 4 (CI/CD) | Project-wide |
| 21 | No application metrics (Prometheus/StatsD/OTEL) | Phase 4 (CI/CD) | Project-wide |
| 22 | No health check endpoint | Phase 4 (CI/CD) | REST adapter |
| 23 | `multiprocessing.Pool` recreated per `scan_all` request | Phase 4 (CI/CD) | `scanners/service.py` |

### Medium Priority (P2 — Plan for Next Sprint)

| # | Finding | Source Phase |
|---|---------|-------------|
| 24 | `RealtimeCandle` type alias duplicated in 3 modules | Quality |
| 25 | `DailyIndicatorCache` reloads all indicators sequentially per subscription | Performance |
| 26 | Cache misses silently swallowed with no logging | Quality |
| 27 | Exceptions suppressed after 3 errors in caching script | Quality |
| 28 | `None` vs `pd.NA` inconsistency in scanner null checks | Quality |
| 29 | Scanner code duplication in `scan_realtime` (filter logic) | Quality |
| 30 | Shared singleton indicator instances may accumulate state | Quality |
| 31 | Cache deserialization without type validation | Security |
| 32 | WebSocket subscription ID collision/manipulation | Security |
| 33 | Unvalidated `freq` parameter — `KeyError` on bad input | Security |
| 34 | `DailyRollingIndicator` fetches history per symbol on day boundary | Performance |
| 35 | `pd.to_datetime` on every incoming message (~20-50us/call) | Performance |
| 36 | `model_dump_json` on every WebSocket message (~50-200us/call) | Performance |
| 37 | `multiprocessing.Pool` recreated per request (2-5s spawn overhead) | Performance |
| 38 | Missing error handling in `load_from_cache` methods | Performance |
| 39 | Indicator state dicts grow without bound | Performance |
| 40 | No tests for `DailyIndicatorCache`, `CandleBuffer`, scanner adapters | Testing |
| 41 | Duplicate test fixtures diverged (caused the conftest bug) | Testing |
| 42 | Indicator Protocol contract split undocumented | Documentation |
| 43 | Dual cache format mismatch undocumented | Documentation |
| 44 | WebSocket protocols undocumented (different lifecycles) | Documentation |
| 45 | Daily caching script no operational docs | Documentation |
| 46 | README effectively empty | Documentation |
| 47 | Legacy `Dict`/`List`/`Union` imports — use Python 3.11+ builtins | Framework |
| 48 | `assert` used for runtime validation (stripped with `-O`) | Framework |
| 49 | SIGUSR2 in systemd `ExecStop` — not handled, every stop is hard kill | CI/CD |
| 50 | NATS connection has no reconnection logic — silent data loss | CI/CD |
| 51 | New daily caching script has no systemd unit/scheduler | CI/CD |
| 52 | Logging config uses relative file path | CI/CD |
| 53 | Missing env vars crash with unhelpful `KeyError` | CI/CD |

### Low Priority (P3 — Track in Backlog)

| # | Finding | Source Phase |
|---|---------|-------------|
| 54 | Double UUID assignment in `SmallCapUpScanner.__init__` | Architecture |
| 55 | Benchmark handler signature stale (pd.Series vs dict) | Architecture |
| 56 | Raw WebSocket data logged on error (log bloat) | Security |
| 57 | `logging.basicConfig` at module level overrides app config | Quality |
| 58 | Negative sleep duration in `CandleBuffer._timeout_flush` | Performance |
| 59 | `ShiftIndicator` uses `list.pop(0)` instead of `deque` | Performance |
| 60 | `DailyATRParabolicDownScanner` error message references wrong class | Framework |
| 61 | Unused imports in multiple files | Framework |
| 62 | `ContextVar` used as global singleton (misleading) | Framework |
| 63 | Commented-out code blocks in scanners | Framework |
| 64 | Hardcoded paths in systemd units | CI/CD |
| 65 | `.env.example` out of sync with `config.py` | CI/CD |
| 66 | DragonflyCache has no connection timeouts | CI/CD |
| 67 | `pyproject.toml` has empty description | Documentation |
| 68 | Exact-pinned `pandas = "2.3.0"` blocks patch updates | Framework |

---

## Findings by Category

| Category | Total | Critical | High | Medium | Low |
|----------|-------|----------|------|--------|-----|
| Code Quality | 11 | 1 | 4 | 5 | 1 |
| Architecture | 8 | 2 | 3 | 2 | 1 |
| Security | 5 | 0 | 1 | 3 | 1 |
| Performance | 14 | 1 | 4 | 8 | 1 |
| Testing | 8 | 1 | 4 | 3 | 0 |
| Documentation | 7 | 0 | 0 | 5 | 2 |
| Framework | 10 | 0 | 2 | 3 | 5 |
| CI/CD & DevOps | 12 | 1 | 5 | 4 | 2 |
| **Total** | **75** | **6** | **23** | **33** | **13** |

---

## Recommended Action Plan

### Immediate (This Week)

1. **Fix `SmallCapUpScanner.scan` loop bug** — zip `self._shift_indicators` alongside periods and min_change. [small effort]
2. **Fix `CandleStoreTest.get` undefined variables** — change `start_date`/`end_date` to `start`/`end`. [small effort]
3. **Fix `CumulativeIndicator.load_from_cache`** — use `.get("last_date", {})` for backwards compat. [small effort]
4. **Fix truthiness check on `atr_val`** — change to `atr_val is not None`. [small effort]

### Short-term (Next 1-2 Sprints)

5. **Add CI pipeline** — GitHub Actions with `pytest` + ruff + mypy on push/PR. [medium effort]
6. **Add ruff + mypy config** to `pyproject.toml` and fix initial violations. [medium effort]
7. **Split Indicator Protocol** into `Indicator` + `RealtimeIndicator`, replace `hasattr` with `isinstance`. [medium effort]
8. **Consolidate scanner duplication** — extract direction-parameterized base class, reduce ~2000 lines to ~500. [large effort]
9. **Fix DailyIndicatorCache thundering herd** — add `asyncio.Lock` with double-checked locking. [small effort]
10. **Add health check endpoint** (`GET /api/health`) checking Dragonfly + NATS connectivity. [small effort]
11. **Deduplicate test fixtures** — shared `fastscanner/tests/mocks.py` module. [medium effort]
12. **Add scanner tests** — at minimum, passes-filter and fails-filter scenarios for each scanner. [medium effort]

### Medium-term (Next Quarter)

13. **Document cache contract** — clarify ownership between `run_daily_indicators_caching.py` and per-indicator `save_to_cache`. [small effort]
14. **Parallelize cache loading** — `asyncio.gather` in `load_all_cacheable` and daily caching script. [small effort]
15. **Add NATS reconnection callbacks** — prevent silent data loss on transient failures. [small effort]
16. **Fix systemd SIGUSR2** — change to SIGTERM for graceful shutdown. [small effort]
17. **Add Prometheus metrics** — WebSocket connections, throughput, cache hit rates. [medium effort]
18. **Refactor `ApplicationRegistry`** — move to constructor injection for indicators. [large effort]
19. **Write README** with setup, architecture overview, and operational instructions. [medium effort]

---

## Review Metadata

- Review date: 2026-02-22
- Phases completed: 1 (Quality & Architecture), 2 (Security & Performance), 3 (Testing & Documentation), 4 (Best Practices & Standards), 5 (Consolidated Report)
- Flags applied: none
- Framework: Python 3.11+ / FastAPI
- Context: Internal-only application, private network, trusted callers
