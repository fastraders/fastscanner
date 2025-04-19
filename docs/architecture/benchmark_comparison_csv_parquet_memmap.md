# 100-Request Performance Benchmark: Partitioned CSV vs Parquet vs Memory-Mapped (Memmap)

## Objective

To compare the read performance of three storage formats under a load of **100 consecutive read requests** for OHLC (Open, High, Low, Close) financial data.

The providers tested:
- Partitioned CSV
- Parquet
- Memory-Mapped (Memmap)

---

## Test Setup

- **Symbol**: `AAPL`
- **Date Range**: 2023-01-01 to 2023-03-31
- **Frequency**: `1min`
- **Requests**: 100 consecutive `.get()` calls per provider
- **Machine Specs**: 24 GB RAM
- **Python Version**: 3.11
- **Test Metric**: Total time and per-request average latency

---

## Benchmark Results

### Partitioned CSV Provider

Requests: 100 | Total Time: 2.28 seconds | Avg Per Request: 22.83 ms


---

### Parquet Provider

Requests: 100 | Total Time: 0.36 seconds | Avg Per Request: 3.60 ms


---

### Memory-Mapped Provider

🔁 Requests: 100 | Total Time: 0.26 seconds | Avg Per Request: 2.60 ms

---

## Summary Table

| Provider       | Total Time (s) | Avg Time per Request (ms) |
|----------------|----------------|----------------------------|
| **CSV**        | 2.28           | 22.83                      |
| **Parquet**    | 0.36           | 3.60                       |
| **Memmap**     | 0.26           | 2.60                       |

---
# OHLC Data Read Benchmark Report

**Test Symbol:** AAPL  
**Date Range:** 2023-01-01 to 2023-03-31  
**Frequency:** 1min  
**Read Repeats:** 5  
**Bulk Requests:** 100  

---

## Partitioned CSV (Cold + Warm Read)

### Warm Cache Benchmark (5 Reads)
| Run | Time (s)  | Rows |
|-----|-----------|------|
| 1   | 0.3931    | 48809 |
| 2   | 0.1192    | 48809 |
| 3   | 0.1261    | 48809 |
| 4   | 0.1241    | 48809 |
| 5   | 0.1175    | 48809 |

### Stats
- **Min:** 0.1175s  
- **Max:** 0.3931s  
- **Average:** 0.1760s  
- **Median:** 0.1241s  
- **Avg Rows:** 48809  

### Bulk Read (100 Requests)
- **Total Time:** 13.54s  
- **Avg Time/Request:** 0.1354s  

---

## Parquet (Cold + Warm Read)

### Warm Cache Benchmark (5 Reads)
| Run | Time (s)  | Rows |
|-----|-----------|------|
| 1   | 0.0568    | 48809 |
| 2   | 0.0593    | 48809 |
| 3   | 0.0614    | 48809 |
| 4   | 0.0784    | 48809 |
| 5   | 0.0687    | 48809 |

### Stats
- **Min:** 0.0568s  
- **Max:** 0.0784s  
- **Average:** 0.0649s  
- **Median:** 0.0614s  
- **Avg Rows:** 48809  

### Bulk Read (100 Requests)
- **Total Time:** 6.31s  
- **Avg Time/Request:** 0.0631s  

---

## Memmap (Cold + Warm Read)

### Warm Cache Benchmark (5 Reads)
| Run | Time (s)  | Rows |
|-----|-----------|------|
| 1   | 0.5521    | 48809 |
| 2   | 0.6075    | 48809 |
| 3   | 0.5343    | 48809 |
| 4   | 0.5554    | 48809 |
| 5   | 0.5656    | 48809 |

### Stats
- **Min:** 0.5343s  
- **Max:** 0.6075s  
- **Average:** 0.5630s  
- **Median:** 0.5554s  
- **Avg Rows:** 48809  

### Bulk Read (100 Requests)
- **Total Time:** 56.84s  
- **Avg Time/Request:** 0.5684s  

---

## Summary

| Format           | Min   | Max   | Avg   | Bulk Avg |
|------------------|--------|--------|--------|-----------|
| Partitioned CSV  | 0.1175 | 0.3931 | 0.1760 | 0.1354s   |
| Parquet          | 0.0568 | 0.0784 | 0.0649 | 0.0631s   |
| Memmap           | 0.5343 | 0.6075 | 0.5630 | 0.5684s   |

**Fastest Format:** **Parquet**  





