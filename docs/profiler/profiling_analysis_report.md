# FastScanner Profiling Analysis Report

**Analysis Date:** Generated from Austin Profiler Results  
**Profiling Tool:** Austin (austin-web)

## Executive Summary

This report summarizes the key performance bottlenecks and optimization opportunities for FastScanner's batch and realtime scanning components, based on Austin profiler flame graphs. The focus is on actionable insights for future improvements.

## 1. Profiling Methodology

- **Tool Used:** Austin Web (recommended for both batch and realtime profiling)
- **How to Profile:**
  1. Run the appropriate `make` command (see runbook for details).
  2. Open the Austin Web interface and analyze the flame graph.
  3. Focus on wide blocks under project code (ignore system/runner functions).

## 2. Normal Scanner (benchmark_scanner)
![Benchmark Scanner Flame Graph][normal-scanner-flame]

### 2.1 Key Bottlenecks (from Flame Graph)
- **CSV File Reading (`PartitionedCSVCandlesProvider.read_csv` and related):**
  - Dominates total runtime (~60-70%).
  - This is expected due to the large volume of data processed sequentially.
  - **Optimization:** Consider switching to a faster format (e.g., Parquet), parallelizing reads, if further speedup is needed, but for our use case as the data is not much we are using partitioned csv because parquet work better for larger dataset.
- **Indicator Calculations:**
  - ~10-15% of time spent in technical indicator functions.
  - No single indicator dominates; further profiling can target individual indicators if needed.
### 2.2 Conclusion
- **Main bottleneck is file I/O, which is expected for batch processing.**
- No major inefficiencies found in business logic.
- For significant speedup, focus on optimizing data access (format).

## 3. Realtime Scanner (benchmark_scanner_realtime)
![Realtime Benchmark Scanner Flame Graph][realtime-flame]

### 3.1 Key Bottlenecks (from Flame Graph)
- **Redis Stream Reading (`RedisChannel._xread_loop`):**
  - 40-50% of time spent on consuming messages from Redis.
  - This is expected for a streaming/event-driven architecture.
- **Event Processing (`ScannerChannelHandler.handle`):**
  - 25-35% of time spent on message handling and routing.
- **Indicator Calculations:**
  - 15-25% of time spent in real-time indicator computations.


**Batch Summary Statistics:**
![Realtime Scanner Logs][realtime-logs]

| Batch   | Messages | Time (s) | Rate (msg/sec) |
|---------|----------|----------|---------------|
| Batch 1 | 278      | 3.29     | 44.3          |
| Batch 2 | 319      | 2.00     | 59.9          |
| Batch 3 | 295      | 3.00     | 31.9          |
| Batch 4 | 315      | 3.00     | 44.1          |
| Batch 5 | 316      | 2.40     | 45.9          |

# Batch Summary Conclusion
When the function starts, the batch time is slightly higher. However, as processing continues, the performance stabilizes, and subsequent batches show more consistent throughput.
the message processing rate becomes more stable, with batches processing messages at rates between 31.9 to 59.9 msg/sec. This suggests that the    system adjusts after the initial batch, maintaining a more efficient processing time for subsequent batches.

In summary, the initial batch time is a bit higher, but the system stabilizes quickly, leading to consistent batch processing times thereafter
### 3.2 Conclusion
- **Performance is dominated by I/O (Redis) and event processing, as expected.**
- No single indicator or handler is a clear bottleneck.
- For further optimization, consider:
  - Reducing Redis/network latency (if possible)
  - Profiling individual indicators if real-time throughput is insufficient

## 4. Comparative Analysis

| Aspect | Batch Scanner | Realtime Scanner |
|--------|--------------|-----------------|
| **Processing Model** | Batch (sequential) | Stream (event-driven) |
| **Data Source** | CSV files | Redis streams |
| **Main Bottleneck** | File I/O | Redis I/O, event handling |
| **Throughput** | ~6.25 symbols/sec | ~45 events/sec |
| **Optimization Focus** | Data access | Network/event handling |

## 5. Recommendations
- Focus on optimizing data access (batch) and event/network handling (realtime) for future improvements.
- Ignore system/runner functions in flame graphs; focus on wide blocks under your own code.

---
<!-- Image References -->
[normal-scanner-flame]: ../../docs/images/benchmark_scanner_austin_web.png "Normal Scanner Flame Graph - Austin Web"
[realtime-flame]: ../../docs/images/realtime_scanner_flame_graph.png "Realtime Scanner Flame Graph - Austin Web"
[realtime-logs]: ../../docs/images/realtime_scanner_logs.png "Realtime Scanner Batch Processing Logs"
*For step-by-step profiling instructions, see the runbook.*