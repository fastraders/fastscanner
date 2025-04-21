# Benchmark Comparison: Partitioned CSV vs Parquet

## Overview

We benchmarked data access performance between two storage formats — **Partitioned CSV** and **Parquet with native partitioning** — across various time ranges and frequencies. The following metrics compare rows returned and read times (in seconds) for each format.

## Results Summary

| Range     | CSV Rows | CSV Time (s) | Parquet Rows | Parquet Time (s) |
|-----------|----------|--------------|---------------|------------------|
| 1min-1D   | 770      | 0.0085       | 770           | 0.0833           |
| 1min-1M   | 16079    | 0.0390       | 16079         | 0.0893           |
| 1min-1Y   | 203262   | 0.3953       | 203262        | 0.1607           |
| 1min-3Y   | 615119   | 1.1731       | 615119        | 0.3400           |
| 2min-1D   | 422      | 0.0116       | 422           | 0.0863           |
| 2min-1M   | 8859     | 0.0398       | 8859          | 0.0917           |
| 2min-1Y   | 109503   | 0.4270       | 109503        | 0.1484           |
| 2min-3Y   | 332630   | 1.3011       | 332630        | 0.3107           |
| 15min-1D  | 64       | 0.0087       | 64            | 0.0909           |
| 15min-1M  | 1338     | 0.0391       | 1338          | 0.0957           |
| 15min-1Y  | 16047    | 0.4103       | 16047         | 0.1410           |
| 15min-3Y  | 48142    | 1.2840       | 48142         | 0.2466           |
| 1h-1D     | 16       | 0.0025       | 16            | 0.0858           |
| 1h-1M     | 336      | 0.0033       | 336           | 0.0916           |
| 1h-1Y     | 4038     | 0.0269       | 4038          | 0.1384           |
| 1h-3Y     | 12076    | 0.0698       | 12076         | 0.2509           |
| 5h-1D     | 4        | 0.0044       | 3             | 0.0876           |
| 5h-1M     | 84       | 0.0064       | 70            | 0.0845           |
| 5h-1Y     | 1011     | 0.0282       | 801           | 0.1322           |
| 5h-3Y     | 3021     | 0.0722       | 2371          | 0.2453           |
| 1d-1D     | 1        | 0.0024       | 1             | 0.0920           |
| 1d-1M     | 21       | 0.0027       | 21            | 0.0990           |
| 1d-1Y     | 253      | 0.0207       | 253           | 0.1416           |
| 1d-3Y     | 756      | 0.0578       | 756           | 0.2601           |

## Key Observations

- **Partitioned CSV consistently reads faster** than Parquet for small-to-medium datasets.
- For larger datasets (e.g., `1min-3Y`), **Parquet performs better**.
- **Parquet shows overhead** especially in small queries like `5h-1D`, `1d-1D`, etc.

## Decision

Given our current usage involves **relatively small datasets (1-2 days of intraday data)**, we are moving forward with the **Partitioned CSV implementation** for now.

It offers:

- Faster read performance in short intervals

We’ll revisit Parquet when:

- Our data volume grows significantly
- We have longer backfill periods (multi-month or year)
