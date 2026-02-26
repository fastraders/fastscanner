# Phase 1: Code Quality & Architecture Review

## Code Quality Findings

### Critical

1. **Indicator Protocol declares `extend_realtime` but several implementations lack it** (`lib/__init__.py`, daily indicators)
   - `PrevDayIndicator`, `DailyATRIndicator`, `DailyATRGapIndicator`, `ADRIndicator`, `ADVIndicator` no longer have `extend_realtime`. They don't satisfy the `Indicator` protocol. Code works only because `hasattr` is used at the call-site. Any code accepting `Indicator` and calling `extend_realtime` will fail at runtime for daily-only indicators.
   - **Fix**: Split into `Indicator` and `RealtimeIndicator` protocols.

### High

2. **`hasattr` check for protocol conformance is fragile** (`service.py:97`)
   - Duck-typing via `hasattr(ind, "extend_realtime")` defeats static type checking and can silently pass if a class has a broken `extend_realtime`.
   - **Fix**: Use `isinstance(ind, RealtimeIndicator)` with the split protocol.

3. **Truthiness check on `atr_val` conflates `0` and `None`** (`candle.py`, ATRGapIndicator.extend_realtime)
   - `if atr_val and ...` uses truthiness, meaning `atr_val = 0` evaluates as `False`. Mixes two null-check idioms in the same expression.
   - **Fix**: Use `if atr_val is not None and ...` consistently.

4. **`CumulativeIndicator.load_from_cache` crashes on old cache entries lacking `last_date`** (`candle.py`)
   - `save_to_cache` now saves `last_date`, but `load_from_cache` unconditionally accesses `indicator_data["last_date"]`. Old cache entries will cause `KeyError`.
   - **Fix**: Use `indicator_data.get("last_date", {})`.

### Medium

5. **`RealtimeCandle` type alias duplicated in 3 modules** (`pkg/candle.py`, `indicators/service.py`, `scanners/service.py`)
   - **Fix**: Define once in a shared location (e.g., `ports.py`) and import everywhere.

6. **`DailyIndicatorCache` reloads all indicators sequentially per subscription per day** (`service.py:311-327`)
   - Each handler creates its own cache instance. On day boundary, all subscriptions independently reload all indicators sequentially.
   - **Fix**: Share a single cache instance, or batch reads with `asyncio.gather`.

7. **Cache misses silently swallowed with no logging** (`service.py:311-317`)
   - If a cache key is consistently missing due to misconfigured caching job, there's no way to detect from logs.
   - **Fix**: Log a warning on cache miss.

8. **Exceptions silently suppressed after 3 errors in caching script** (`run_daily_indicators_caching.py:71`)
   - After 3 errors per indicator, all subsequent exceptions are swallowed. Systemic issues lead to silently partial cache data.
   - **Fix**: Always log at minimum level; consider abort threshold.

9. **`None` vs `pd.NA` inconsistency in scanner null checks** (scanner lib files)
   - Scanners use `v is None` but batch path uses `pd.NA`. `PositionInRangeIndicator` handles both, but this isn't applied consistently.
   - **Fix**: Standardize on `None` for realtime path and document the contract.

10. **Heavy code duplication in `scan_realtime` across 6 scanner classes** (scanner lib files)
    - `market_cap_passes`, `days_from_earnings_passes`, time filtering all copy-pasted verbatim.
    - **Fix**: Extract shared filter logic into a base class or mixin.

11. **Shared singleton indicator instances may accumulate state** (`run_daily_indicators_caching.py`)
    - `DAILY_INDICATORS` is a module-level list of singletons reused across all symbols. Some indicators update instance state during `extend()`.
    - **Fix**: Create fresh indicator instances in the caching script or add a factory function.

12. **`_row_to_dataframe` gives no context on missing timestamp** (`candle.py:27-30`)
    - Raw `KeyError` with no useful context at a critical integration point.

### Low

13. **Possible negative sleep duration** (`candle.py:86`) — guard with `max(0, ...)`.
14. **Truthiness check on timestamp instead of `is not None`** (`scanner.py:42`)
15. **`logging.basicConfig` at module level overrides app config** (`service.py:20`)
16. **Missing `cache.close()` in error path** (`run_daily_indicators_caching.py:84`) — use `try/finally`.
17. **Cancelled timeout tasks not awaited** (`candle.py:57`) — acceptable due to lock pattern; no change needed.

---

## Architecture Findings

### Critical

1. **`SmallCapUpScanner.scan` shift column reference bug** (`smallcap.py:163-168`)
   - The `scan` method references `shift_indicator.column_name()` bound to the last element from a previous loop, so all shift comparisons use the wrong column. Runtime logic error.
   - **Fix**: Zip `self._shift_indicators` alongside `self._shift_periods` and `self._shift_min_change`.

2. **`CandleStoreTest.get` references undefined variables** (`scanners/tests/conftest.py:145-158`)
   - Method parameters are `start` and `end` but body references `start_date` and `end_date`. `NameError` at runtime.
   - **Fix**: Use correct parameter names.

### High

3. **`ApplicationRegistry` as global service locator** (indicator classes)
   - Indicators directly access `ApplicationRegistry.candles` and `ApplicationRegistry.cache` as global state. Violates Dependency Inversion. Tests require initializing global registry. Parallel test execution unsafe.
   - **Fix**: Pass `CandleStore` and `Cache` via constructor parameters.

4. **`DailyIndicatorCache` / per-indicator cache format mismatch** (`service.py`, `daily.py`)
   - `DailyIndicatorCache` expects flat `{symbol: value}` dicts. Per-indicator `save_to_cache` methods save nested structures (`{"prev_day": {...}, "last_date": {...}}`). Contract mismatch between the two cache writers.
   - **Fix**: Standardize cache format or use separate key namespaces.

5. **Massive scanner code duplication (~2000 lines)** (gap.py, parabolic.py, range_gap.py)
   - Up/Down scanner pairs are ~95% identical. Any pipeline change must be replicated across all variants.
   - **Fix**: Extract a direction-parameterized base scanner class.

### Medium

6. **Indicator lib imports from service layer** (`candle.py:23`)
   - Library layer references service layer via `TYPE_CHECKING` import. Breaks proper layering.
   - **Fix**: Move `RealtimeCandle` to ports module.

7. **Scanner classes directly instantiate indicator internals** (gap.py, parabolic.py)
   - Tight coupling between scanner and indicator services. Internal indicator refactoring breaks scanner code.
   - **Fix**: Consider an indicator factory/builder interface.

8. **`ScannerService` imports from indicators service implementation** (`scanners/service.py:16`)
   - Direct dependency on indicators service internals, not just ports.
   - **Fix**: Extract `DailyIndicatorCache` to shared module; pass `DAILY_INDICATOR_COLUMNS` as constructor parameter.

9. **Missing `extend_realtime` with duck-typing fallback** (daily indicators)
   - Reliance on `hasattr` rather than proper type hierarchy makes the contract unclear.
   - **Fix**: Introduce `BatchIndicator` and `RealtimeIndicator` protocols.

10. **Benchmark uses wrong channel implementation** (`benchmark_scanner_realtime.py:21`)
    - Benchmark uses `RedisChannel` while production uses `NATSChannel`. May not represent production behavior.

11. **Duplicate mock classes across test modules** (4+ test files)
    - `MockCache`, `MockChannel`, etc. duplicated with slightly different implementations.
    - **Fix**: Create shared `fastscanner/tests/mocks.py`.

12. **REST calculate endpoint returns raw CSV string** (`indicators.py:103-120`)
    - Bypasses FastAPI content negotiation and documentation.
    - **Fix**: Use `StreamingResponse` with `media_type="text/csv"`.

### Low

13. **Double UUID assignment in `SmallCapUpScanner.__init__`** (`smallcap.py:41,44`)
14. **Benchmark handler signature mismatch** (`benchmark_scanner_realtime.py:42`)
15. **Deleted `run_data_provider.py` without migration check**

---

## Critical Issues for Phase 2 Context

- **Cache format mismatch** (Arch 4) may cause silent data corruption — security/integrity concern.
- **`SmallCapUpScanner.scan` bug** (Arch 1) produces incorrect scan results — correctness risk.
- **`CandleStoreTest` undefined variables** (Arch 2) means scanner tests may not be running correctly.
- **`DailyIndicatorCache` sequential reload** (Quality 6) — performance bottleneck at day boundaries.
- **Scanner code duplication** (Arch 5) — large attack surface for inconsistencies.
- **`CumulativeIndicator.load_from_cache` backwards compatibility** (Quality 4) — deployment risk.
