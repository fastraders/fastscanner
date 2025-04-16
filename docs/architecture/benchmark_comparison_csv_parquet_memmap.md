# Benchmark Report: CSV vs Parquet vs Memmap for OHLC Data Reads

## Objective

The objective of this benchmark is to evaluate and compare the read performance of three different OHLC data storage approaches used in our system:

- **Partitioned CSV Provider**
- **Partitioned Parquet Provider**
- **Partitioned Memmap Provider**

These storage formats are used to cache and serve historical OHLC (Open, High, Low, Close) financial data. Performance is critical due to high-frequency read operations required during backtesting and trading simulations.

---

## Test Environment

- **Machine Specs**: 24 GB RAM
- **Python Version**: 3.11
- **Libraries**: pandas, pyarrow, numpy, etc.
- **Dataset**: ~48,809 rows of minute-level OHLC data for a single symbol
- **Read Frequency**: 5 consecutive reads per provider
- **Local Timezone**: `LOCAL_TIMEZONE_STR`

---

## Benchmark Results

### 1. CSV Provider


| Metric     | Time (s) |
|------------|----------|
| Min        | 0.1122   |
| Max        | 0.1163   |
| Average    | 0.1135   |
| Median     | 0.1129   |

---

### 2. Parquet Provider


| Metric     | Time (s) |
|------------|----------|
| Min        | 0.0685   |
| Max        | 0.0850   |
| Average    | 0.0769   |
| Median     | 0.0744   |

---

### 3. Memmap Provider


| Metric     | Time (s) |
|------------|----------|
| Min        | 0.0886   |
| Max        | 0.2607   |
| Average    | 0.1311   |
| Median     | 0.1083   |

---

## Summary

| Provider   | Avg Read Time (s) | Median (s) |
|------------|-------------------|------------|
| **CSV**    | 0.1135            | 0.1129     | 
| **Parquet**| **0.0769**        | **0.0744** | 
| **Memmap** | 0.1311            | 0.1083     | 

### Best Choice: **Parquet**

- **Parquet** consistently offers the best performance for high-throughput, low-latency workloads.
- It supports better compression and faster read speeds compared to CSV and Memmap.
- While Memmap has potential for performance, it showed variability in initial access latency and higher complexity in managing `.meta` and `.dat` files.


