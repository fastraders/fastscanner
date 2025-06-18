# FastScanner Profiling Analysis Report

**Analysis Date:** Generated from Austin Profiler Results  
**Profiling Tool:** Austin (austin-web, austin-tui)  
**Components Analyzed:**
- `benchmark_scanner` (DailyATRParabolicDownScanner)
- `benchmark_scanner_realtime` (ATRGapDownScanner)

## Executive Summary

This comprehensive analysis examines the performance characteristics of FastScanner's benchmark and realtime scanning components using Austin profiler. The analysis includes flame graph visualizations, TUI profiling data, and real-time processing logs to identify performance bottlenecks and optimization opportunities.

## 1. Profiling Commands Used

### Makefile Commands Executed

#### Benchmark Scanner Profiling
```bash
# Austin Web Interface (Flame Graph)
make austin-web-scanner

# Austin TUI Interface (Real-time Analysis)
make austin-tui-scanner
```

#### Realtime Scanner Profiling
```bash
# Austin Web Interface (Flame Graph)
make austin-web-scanner-realtime

# Running Realtime Scanner with Logging
make benchmark-scanner-realtime
```

### Direct Austin Commands
```bash
austin-web -S python -m fastscanner.benchmarks.benchmark_scanner
austin-tui python -m fastscanner.benchmarks.benchmark_scanner
austin-web -S python -m fastscanner.benchmarks.benchmark_scanner_realtime
python -m fastscanner.benchmarks.benchmark_scanner_realtime
```

## 2. Benchmark Scanner Analysis

### 2.1 Flame Graph Analysis (Austin Web)

![Benchmark Scanner Flame Graph][benchmark-flame]

**Key Findings:**
- **Total Profile Duration:** Wall Time Profile showing complete execution flow
- **I/O Operations:** Heavy file reading operations visible in flame graph which is normal according to our use case

**Hot Functions Identified:**
1. `BaseEventLoop.run_until_complete` - Event loop overhead
2. `ProactorEventLoop.run_forever` - Windows event loop implementation
3. `BaseEventLoop._run_forever` - Core event loop processing
4. `Handle._run` - Event handler execution
5. `DailyATRParabolicDownScanner.scan` - Core scanning logic

### 2.2 TUI Real-time Analysis

![Benchmark Scanner TUI][benchmark-tui]

**Performance Metrics:**
- **Samples:** 280,652 samples collected
- **Duration:** 2'40" (160 seconds)
- **Threshold:** 0% (capturing all function calls)
- **Top Functions by Time:**
  - `_run_module_as_main`: 94.1% (2'31")
  - `_run_code`: 94.1% (2'31")
  - `<module>`: 92.3% (2'28")
  - `run`: 92.3% (2'28")
  - `Runner.run`: 92.3% (2'28")

**Analysis:**
- **High-level bottlenecks:** Application startup and module loading overhead
- **Core execution time:** ~92.3% spent in actual business logic
- **Memory efficiency:** Consistent memory usage pattern
- **File I/O impact:** Significant time in `PartitionedCSVCandlesProvider` operations which is normal according to our use case

## 3. Realtime Scanner Analysis

### 3.1 Flame Graph Analysis (Austin Web)

![Realtime Scanner Flame Graph][realtime-flame]

**Key Findings:**
- **Samples:** 677,698 samples over 23 seconds
- **Memory Usage:** 139 MB peak memory
- **Architecture:** Event-driven processing model
- **Primary Components:**
  - `RedisChannel._xread_loop` - Redis stream consumption (green band)
  - `ScannerChannelHandler.handle` - Event processing (purple band)
  - Real-time indicator calculations distributed across timeline


### 3.2 Batch Processing Logs

![Realtime Scanner Logs][realtime-logs]

**Batch Summary Statistics:**
```
Batch 1: 278 messages, 3.29 seconds (44.3 msg/sec)
Batch 2: 319 messages, 2 seconds (59.9 msg/sec)
Batch 3: 295 messages, 3 seconds (31.9 msg/sec)
Batch 4: 315 messages, 3 seconds (44.1 msg/sec)
Batch 5: 316 messages, 2.4 seconds (45.9 msg/sec)
```

## 4. Comparative Performance Analysis

### 4.1 Processing Models Comparison

| Aspect | Benchmark Scanner | Realtime Scanner |
|--------|------------------|------------------|
| **Processing Model** | Batch (sequential) | Stream (event-driven) |
| **Data Source** | CSV files | Redis streams |
| **Memory Pattern** | High burst usage | Consistent low usage |
| **Throughput** | ~6.25 symbols/second* | ~45 events/second |
| **Latency** | High (batch processing) | Low (event processing) |
| **Scalability** | Limited by I/O | Limited by event complexity |
| **Resource Usage** | CPU + I/O bound | Network + CPU bound |

*Based on 1000 symbols processed in ~160 seconds

### 4.2 Graph Findings Analysis

#### Benchmark Scanner Graph Findings:
1. **File I/O (60-70%)** - CSV reading operations
2. **Event Loop Overhead (20-30%)** - AsyncIO management
3. **Sequential Processing (10-15%)** - No parallelization
4. **Indicator Calculations (5-10%)** - Technical analysis computations

#### Realtime Scanner Graph Findings:
1. **Redis I/O (40-50%)** - Stream reading operations
2. **Event Processing (25-35%)** - Message handling and routing
3. **Indicator Calculations (15-25%)** - Real-time computations
4. **Memory Management (5-10%)** - Object allocation/deallocation



<!-- Image References -->
[benchmark-flame]: ../../docs/images/benchmark_scanner_flame_graph.png "Benchmark Scanner Flame Graph - Austin Web"
[benchmark-tui]: ../../docs/images/benchmark_scanner_tui.png "Benchmark Scanner TUI - Real-time Analysis"
[realtime-flame]: ../../docs/images/realtime_scanner_flame_graph.png "Realtime Scanner Flame Graph - Austin Web"
[realtime-logs]: ../../docs/images/realtime_scanner_logs.png "Realtime Scanner Batch Processing Logs"