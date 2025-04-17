import time
import statistics
from datetime import date

import pandas as pd

from fastscanner.adapters.candle.partitioned_csv import PartitionedCSVBarsProvider
from fastscanner.adapters.candle.parquet import ParquetBarsProvider
from fastscanner.adapters.candle.memmap import MemmapBarsProvider

SYMBOL = "AAPL"
START_DATE = date(2023, 1, 1)
END_DATE = date(2023, 3, 31)
FREQ = "1min"  # You can also test with "1h" or "1d"
REPEATS = 5
BULK_REQUESTS = 100


def benchmark(provider_cls, label):
    print(f"\nPriming {label} (cold read)...")
    provider = provider_cls()
    provider.get(SYMBOL, START_DATE, END_DATE, FREQ)

    print(f"\nBenchmarking {label} for {REPEATS} repeated reads (warm cache)...")
    times = []
    row_counts = []

    for i in range(REPEATS):
        start = time.perf_counter()
        df = provider.get(SYMBOL, START_DATE, END_DATE, FREQ)
        end = time.perf_counter()

        duration = end - start
        times.append(duration)
        row_counts.append(len(df))

        print(f"[{label}] Run {i+1}: {duration:.4f}s — Rows: {len(df)}")

    print(f"\n{label} Stats ")
    print(f"Min:     {min(times):.4f}s")
    print(f"Max:     {max(times):.4f}s")
    print(f"Average: {statistics.mean(times):.4f}s")
    print(f"Median:  {statistics.median(times):.4f}s")
    print(f"Avg Rows: {int(statistics.mean(row_counts))}")
    print("-" * 40)

    print(f"\nBulk Read Test ({BULK_REQUESTS} requests) — {label}")
    start = time.perf_counter()
    for _ in range(BULK_REQUESTS):
        provider.get(SYMBOL, START_DATE, END_DATE, FREQ)
    end = time.perf_counter()
    total_time = end - start
    print(f"Total Time:{total_time:.2f} seconds")
    print(f"Avg Time/Request: {total_time / BULK_REQUESTS:.4f}s")
    print("=" * 50)


if __name__ == "__main__":
    benchmark(PartitionedCSVBarsProvider, "Partitioned CSV")
    benchmark(ParquetBarsProvider, "Parquet")
    benchmark(MemmapBarsProvider, "Memmap")
