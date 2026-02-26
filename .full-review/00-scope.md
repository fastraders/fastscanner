# Review Scope

## Target

All modified and untracked files in the working tree — covering changes across adapters, services (indicators & scanners), benchmarks, and shared packages.

## Files

### Modified Files
- fastscanner/adapters/cmd/run_data_provider.py (deleted)
- fastscanner/adapters/rest/indicators.py
- fastscanner/adapters/rest/init.py
- fastscanner/adapters/rest/scanner.py
- fastscanner/adapters/rest/tests/test_indicators.py
- fastscanner/benchmarks/benchmark_scanner_realtime.py
- fastscanner/pkg/candle.py
- fastscanner/services/indicators/lib/__init__.py
- fastscanner/services/indicators/lib/candle.py
- fastscanner/services/indicators/lib/daily.py
- fastscanner/services/indicators/ports.py
- fastscanner/services/indicators/service.py
- fastscanner/services/indicators/tests/test_candle_indicators.py
- fastscanner/services/indicators/tests/test_cumulative_indicators.py
- fastscanner/services/indicators/tests/test_daily_atr_gap_indicator.py
- fastscanner/services/indicators/tests/test_daily_indicators.py
- fastscanner/services/indicators/tests/test_daily_rolling_indicator.py
- fastscanner/services/indicators/tests/test_position_in_range.py
- fastscanner/services/indicators/tests/test_scanner.py
- fastscanner/services/indicators/tests/test_subscribe_realtime.py
- fastscanner/services/scanners/lib/gap.py
- fastscanner/services/scanners/lib/parabolic.py
- fastscanner/services/scanners/lib/range_gap.py
- fastscanner/services/scanners/lib/smallcap.py
- fastscanner/services/scanners/ports.py
- fastscanner/services/scanners/service.py
- fastscanner/services/scanners/tests/conftest.py

### New (Untracked) Files
- fastscanner/adapters/cmd/run_daily_indicators_caching.py
- fastscanner/services/indicators/daily_indicators.py

## Flags

- Security Focus: no
- Performance Critical: no
- Strict Mode: no
- Framework: Python/FastAPI (auto-detected)

## Review Phases

1. Code Quality & Architecture
2. Security & Performance
3. Testing & Documentation
4. Best Practices & Standards
5. Consolidated Report
