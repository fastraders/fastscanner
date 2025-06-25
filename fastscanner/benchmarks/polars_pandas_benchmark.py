import asyncio
import multiprocessing
import os
import time
from concurrent.futures import ProcessPoolExecutor
from datetime import date, timedelta
from itertools import product
from typing import Any, Dict, List

import pandas as pd
import polars as pl

from fastscanner.adapters.candle.partitioned_csv import PartitionedCSVCandlesProvider
from fastscanner.adapters.candle.partitioned_csv_polars import (
    PartitionedCSVCandlesProviderPolars,
)
from fastscanner.adapters.candle.polygon import PolygonCandlesProvider
from fastscanner.pkg import config
from fastscanner.pkg.clock import ClockRegistry, LocalClock

SYMBOL = "AAPL"
START_DATE = date(2020, 1, 5)
FREQUENCIES = ["1min", "2min", "15min", "1h", "5h", "1d"]

DURATION_MAP = {
    "1D": timedelta(days=1),
    "1M": timedelta(days=30),
    "3M": timedelta(days=90),
    "1Y": timedelta(days=365),
}

OUTPUT_DIR = "output"
os.makedirs(OUTPUT_DIR, exist_ok=True)


def benchmark_pandas_worker(args: tuple) -> Dict[str, Any]:
    """Worker function for pandas benchmarking in separate process"""
    freq, duration_label, delta, n_samples = args

    # Initialize clock registry in worker process
    ClockRegistry.set(LocalClock())

    # Create provider in worker process
    polygon = PolygonCandlesProvider(config.POLYGON_BASE_URL, config.POLYGON_API_KEY)
    provider = PartitionedCSVCandlesProvider(polygon)

    case_name = f"{freq}-{duration_label}"
    start = START_DATE
    end = START_DATE + delta

    async def run_benchmark():
        # Prime cache
        df = await provider.get(SYMBOL, start, end, freq)

        # Run benchmark
        elapsed = 0
        for _ in range(n_samples):
            t0 = time.perf_counter()
            df = await provider.get(SYMBOL, start, end, freq)
            t1 = time.perf_counter()
            elapsed += t1 - t0

        elapsed = round(elapsed / n_samples, 4)
        row_count = len(df)
        memory_mb = round(df.memory_usage(deep=True).sum() / 1024**2, 2)

        return {
            "Format": "Pandas",
            "Range": case_name,
            "Time (s)": elapsed,
            "Rows": row_count,
            "Memory (MB)": memory_mb,
        }

    # Run async function in worker process
    return asyncio.run(run_benchmark())


def benchmark_polars_worker(args: tuple) -> Dict[str, Any]:
    """Worker function for polars benchmarking in separate process"""
    freq, duration_label, delta, n_samples = args

    # Initialize clock registry in worker process
    ClockRegistry.set(LocalClock())

    # Create provider in worker process
    polygon = PolygonCandlesProvider(config.POLYGON_BASE_URL, config.POLYGON_API_KEY)
    provider = PartitionedCSVCandlesProviderPolars(polygon)

    case_name = f"{freq}-{duration_label}"
    start = START_DATE
    end = START_DATE + delta

    async def run_benchmark():
        # Prime cache
        df = await provider.get(SYMBOL, start, end, freq)

        # Run benchmark
        elapsed = 0
        for _ in range(n_samples):
            t0 = time.perf_counter()
            df = await provider.get(SYMBOL, start, end, freq)
            t1 = time.perf_counter()
            elapsed += t1 - t0

        elapsed = round(elapsed / n_samples, 4)
        row_count = len(df)

        # Calculate memory usage for polars DataFrame
        if isinstance(df, pl.DataFrame):
            memory_mb = round(df.estimated_size() / 1024**2, 2)
        else:
            # Fallback if it's somehow a pandas DataFrame
            memory_mb = round(df.memory_usage(deep=True).sum() / 1024**2, 2)

        return {
            "Format": "Polars",
            "Range": case_name,
            "Time (s)": elapsed,
            "Rows": row_count,
            "Memory (MB)": memory_mb,
        }

    # Run async function in worker process
    return asyncio.run(run_benchmark())


async def main():
    # Initialize clock registry in main process
    ClockRegistry.set(LocalClock())

    n_processors = multiprocessing.cpu_count()
    n_processes = 2 * n_processors + 1
    n_samples = 50  # Reduced for multiprocessing

    print(
        f"Running benchmark with {n_processes} processes ({n_processors} CPUs detected)"
    )
    print(f"Each test case will run {n_samples} samples per process")

    # Prepare test cases
    test_cases = list(product(FREQUENCIES, DURATION_MAP.items()))

    # Create arguments for workers
    pandas_args = [
        (freq, duration_label, delta, n_samples)
        for freq, (duration_label, delta) in test_cases
    ]
    polars_args = [
        (freq, duration_label, delta, n_samples)
        for freq, (duration_label, delta) in test_cases
    ]

    results = []

    # Run pandas benchmarks
    print("\n=== Running Pandas Benchmarks ===")
    with ProcessPoolExecutor(max_workers=n_processes) as executor:
        pandas_futures = [
            executor.submit(benchmark_pandas_worker, args) for args in pandas_args
        ]

        for i, future in enumerate(pandas_futures):
            result = future.result()
            results.append(result)
            print(
                f"[Pandas {result['Range']}] {result['Time (s)']:.4f}s — Rows: {result['Rows']} — Memory: {result['Memory (MB)']}MB"
            )

    # Run polars benchmarks
    print("\n=== Running Polars Benchmarks ===")
    with ProcessPoolExecutor(max_workers=n_processes) as executor:
        polars_futures = [
            executor.submit(benchmark_polars_worker, args) for args in polars_args
        ]

        for i, future in enumerate(polars_futures):
            result = future.result()
            results.append(result)
            print(
                f"[Polars {result['Range']}] {result['Time (s)']:.4f}s — Rows: {result['Rows']} — Memory: {result['Memory (MB)']}MB"
            )

    # Create comparison report
    df = pd.DataFrame(results)

    # Pivot data for side-by-side comparison
    pandas_df = df[df["Format"] == "Pandas"].set_index("Range")
    polars_df = df[df["Format"] == "Polars"].set_index("Range")

    # Create side-by-side comparison
    comparison = pd.DataFrame(
        {
            "Range": pandas_df.index,
            "Pandas_Time_s": pandas_df["Time (s)"],
            "Polars_Time_s": polars_df["Time (s)"],
            "Pandas_Memory_MB": pandas_df["Memory (MB)"],
            "Polars_Memory_MB": polars_df["Memory (MB)"],
            "Rows": pandas_df["Rows"],
        }
    ).reset_index(drop=True)

    # Calculate performance ratios
    comparison["Speed_Ratio_Polars_vs_Pandas"] = (
        comparison["Pandas_Time_s"] / comparison["Polars_Time_s"]
    )
    comparison["Memory_Ratio_Polars_vs_Pandas"] = (
        comparison["Polars_Memory_MB"] / comparison["Pandas_Memory_MB"]
    )

    # Add summary statistics
    print("\n=== Performance Summary ===")
    avg_speed_ratio = comparison["Speed_Ratio_Polars_vs_Pandas"].mean()
    avg_memory_ratio = comparison["Memory_Ratio_Polars_vs_Pandas"].mean()

    print(f"Average Speed Ratio (Polars vs Pandas): {avg_speed_ratio:.2f}x")
    print(f"Average Memory Ratio (Polars vs Pandas): {avg_memory_ratio:.2f}x")

    if avg_speed_ratio > 1:
        print(f"🚀 Polars is {avg_speed_ratio:.2f}x faster than Pandas on average")
    else:
        print(f"📊 Pandas is {1/avg_speed_ratio:.2f}x faster than Polars on average")

    if avg_memory_ratio < 1:
        print(
            f"💾 Polars uses {(1-avg_memory_ratio)*100:.1f}% less memory than Pandas on average"
        )
    else:
        print(
            f"💾 Polars uses {(avg_memory_ratio-1)*100:.1f}% more memory than Pandas on average"
        )

    # Save detailed results
    csv_path = os.path.join(OUTPUT_DIR, "polars_pandas_benchmark.csv")
    comparison.to_csv(csv_path, index=False)
    print(f"\nDetailed benchmark results saved to: {csv_path}")

    # Save raw results
    raw_csv_path = os.path.join(OUTPUT_DIR, "polars_pandas_benchmark_raw.csv")
    df.to_csv(raw_csv_path, index=False)
    print(f"Raw benchmark results saved to: {raw_csv_path}")

    # Print top performers
    print("\n=== Top Speed Improvements (Polars vs Pandas) ===")
    top_speed = comparison.nlargest(3, "Speed_Ratio_Polars_vs_Pandas")
    for _, row in top_speed.iterrows():
        print(f"{row['Range']}: {row['Speed_Ratio_Polars_vs_Pandas']:.2f}x faster")

    print("\n=== Top Memory Savings (Polars vs Pandas) ===")
    top_memory = comparison.nsmallest(3, "Memory_Ratio_Polars_vs_Pandas")
    for _, row in top_memory.iterrows():
        savings = (1 - row["Memory_Ratio_Polars_vs_Pandas"]) * 100
        print(f"{row['Range']}: {savings:.1f}% less memory")


if __name__ == "__main__":
    # Set multiprocessing start method for compatibility
    multiprocessing.set_start_method("spawn", force=True)
    asyncio.run(main())
