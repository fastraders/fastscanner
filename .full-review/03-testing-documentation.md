# Phase 3: Testing & Documentation Review

## Test Coverage Findings

### Critical

1. **`CandleStoreTest.get` in scanner conftest uses undefined variables** (`scanners/tests/conftest.py:145-158`)
   - Method params are `start`/`end` but body references `start_date`/`end_date`. All scanner tests using this fixture crash with `NameError` — the entire scanner test layer is broken.
   - **Fix**: Rename parameters or variable references to match.

2. **Zero effective test coverage for scanners layer**
   - Scanner tests depend on the broken `CandleStoreTest` fixture. The only scanner test (`test_scanner_consistency_between_scan_and_scan_realtime`) cannot run. No unit tests exist for any individual scanner class.

3. **No tests for `SmallCapUpScanner.scan` loop bug**
   - The known bug (uses wrong shift indicator variable) has no regression test coverage.

### High

4. **Tautological assertion in `test_atr_extend`** (`test_candle_indicators.py:116-120`)
   - Test compares a list to itself (`expected_first_values` and `actual_values` are the same object). Always passes, verifies nothing.

5. **No tests for `save_to_cache` / `load_from_cache` round-trips**
   - No tests verify that what's saved can be loaded back correctly. The `CumulativeIndicator.load_from_cache` crash on old cache entries (missing `last_date` key) is undetected.

6. **No cache format contract tests**
   - No integration tests verify that `run_daily_indicators_caching.py` output is correctly consumed by `DailyIndicatorCache.enrich`.

7. **No tests for `DailyIndicatorCache`**
   - The new caching layer that enriches realtime candles with daily values has no dedicated tests.

8. **No tests for `run_daily_indicators_caching.py`**
   - The new daily caching script is entirely untested.

9. **No tests for `GapIndicator`, `ATRGapIndicator`, `CumulativeIndicator`**
   - These are used by every scanner but have no direct unit tests. Bugs cascade through the entire scanner layer.

10. **Duplicate test fixtures diverged** (`conftest.py` vs `fixtures.py`)
    - `CandleStoreTest`, `MockCache`, `MockChannel`, etc. are duplicated across indicator and scanner test modules with slightly different implementations — which caused the `start_date`/`end_date` bug.

### Medium

11. **No tests for scanner `scan_realtime` methods** — all 8 scanner classes untested for realtime path.
12. **No tests for `CandleBuffer`** — complex aggregation logic with timeout, dedup, and OHLCV rules tested only indirectly.
13. **No tests for WebSocket scanner adapter** — unlike the indicator adapter which has good tests.
14. **No tests for `ScannerService.scan_all`** — multiprocessing logic untested.
15. **Tests assert on internal state** — several tests check `indicator._last_date`, `indicator._rolling_values` instead of public outputs, creating brittle coupling.
16. **Timing-dependent tests** — `asyncio.sleep(1)` and `asyncio.sleep(0.2)` in timeout tests are flaky on slow CI.
17. **`IndicatorsLibrary.instance` monkey-patched in tests** — fragile, could leak between tests on failure. Use `unittest.mock.patch`.

### Low

18. **No negative input tests** — no tests for invalid params to indicator constructors (negative periods, etc.).
19. **No WebSocket malformed-input tests** — no tests for malformed JSON or oversized payloads.

### Untested Edge Cases (Notable)

- Division by zero in `PositionInRangeIndicator.extend` when high == low (realtime version handles this, batch does not)
- Concurrent `extend_realtime` calls for same symbol
- `CandleBuffer` with out-of-order timestamps
- Indicator with period larger than available data

### Test Pyramid Assessment

| Level | Count | Percentage |
|-------|-------|-----------|
| Unit tests (indicators) | ~65 | ~85% |
| Integration tests (WebSocket adapter) | ~10 | ~13% |
| End-to-end tests | 0 | 0% |
| Automated performance tests | 0 | 0% |

The indicator layer has strong coverage. The scanner layer has zero effective coverage due to the broken fixture. No E2E or automated performance tests exist.

---

## Documentation Findings

### Critical

1. **README is effectively empty** — contains only `# fastscanner`. No setup, architecture, API, or operational docs.

2. **Indicator Protocol contract split undocumented** — `Indicator` declares `extend_realtime` as required but daily-only indicators lack it. The `hasattr` split in `service.py` is a critical architectural decision with no documentation.

3. **Dual cache format mismatch undocumented** — two caching mechanisms use the same `indicator:` key prefix but store incompatible formats. No documentation clarifies ownership or coexistence rules.

4. **WebSocket protocols undocumented** — both indicators and scanner WebSocket endpoints have complex bidirectional protocols with different lifecycles (multi-subscription vs single-subscription) but no message schema or lifecycle docs.

5. **Daily caching script has no operational docs** — `run_daily_indicators_caching.py` needs schedule, dependencies, error behavior, and relationship to in-process caching documented.

6. **`pd.Series` to `dict` migration incomplete and undocumented** — fundamental indicators still use `pd.Series` signatures while candle indicators migrated to `dict`. The benchmark handler is also stale.

### High

7. **Cacheable / CacheableIndicator protocols undocumented** — no docs on cache key format, JSON structure, or when caching happens.
8. **Scanner Protocol hierarchy undocumented** — `Scanner` vs `ScannerRealtime` separation not explained.
9. **Channel/pub-sub architecture undocumented** — channel naming, message format, infrastructure choices not documented.
10. **`daily_indicators.py` role undocumented** — canonical list of daily indicators with no module docstring.
11. **Environment variables undocumented** — 13+ env vars (7 mandatory) with no `.env.example`.
12. **Infrastructure dependencies undocumented** — Redis, Dragonfly, NATS, Polygon, EODHD, S3 all required but only Redis in docker-compose.
13. **Near-zero docstrings** — only 2-3 docstrings across 17+ files.

### Medium

14. **Scanner business logic lacks explanation** — complex trading strategies with multi-step filtering have no class-level descriptions.
15. **`CandleBuffer` aggregation undocumented** — time-based bucketing, OHLCV rules, timeout semantics.
16. **REST endpoints lack OpenAPI descriptions** — `/indicators/calculate` returns CSV (not JSON) which is non-obvious.
17. **Benchmark handler signature stale** — still uses `pd.Series` instead of `dict`.
18. **Duplicate `RealtimeCandle` type alias** — defined in 3 places with no canonical source documented.

### Low

19. **`DailyATRParabolicDownScanner` error message references wrong class** — copy-paste error says "Up" in "Down" scanner.
20. **Commented-out code blocks in scanners** — no explanation of why disabled.
21. **`pyproject.toml` has empty description**.
