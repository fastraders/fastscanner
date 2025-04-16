import time
import statistics
from datetime import date
import pandas as pd

from fastscanner.adapters.candle.partitioned_csv import PartitionedCSVBarsProvider
from fastscanner.adapters.candle.partitioned_parquet import PartitionedParquetBarsProvider
from fastscanner.adapters.candle.partitioned_memmap import PartitionedMemmapBarsProvider

SYMBOL = "AAPL"
START_DATE = date(2023, 1, 1)
END_DATE = date(2023, 3, 31)
FREQ = "1min"
REPEATS = 5  

def benchmark(provider_cls, label):
    provider = provider_cls()
    
    print(f"Priming {label}...")
    provider.get(SYMBOL, START_DATE, END_DATE, FREQ)

    times = []

    for _ in range(REPEATS):
        start = time.perf_counter()
        df = provider.get(SYMBOL, START_DATE, END_DATE, FREQ)
        end = time.perf_counter()
        duration = end - start
        times.append(duration)
        print(f"{label} read in {duration:.4f} seconds. Rows: {len(df)}")

    print(f"\n--- {label} Stats ---")
    print(f"Min: {min(times):.4f}s")
    print(f"Max: {max(times):.4f}s")
    print(f"Avg: {statistics.mean(times):.4f}s")
    print(f"Median: {statistics.median(times):.4f}s")

if __name__ == "__main__":
    print("Benchmarking Partitioned CSV vs Parquet...\n")
    benchmark(PartitionedCSVBarsProvider, "CSV Provider")
    benchmark(PartitionedParquetBarsProvider, "Parquet Provider")
    benchmark(PartitionedMemmapBarsProvider, "Memmap Provider")
