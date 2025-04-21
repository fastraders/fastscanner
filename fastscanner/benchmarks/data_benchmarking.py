import os
import time
import pandas as pd
from datetime import date, timedelta
from itertools import product

from fastscanner.adapters.candle.partitioned_csv import PartitionedCSVBarsProvider
from fastscanner.adapters.candle.parquet import ParquetBarsProvider

SYMBOL = "AAPL"
START_DATE = date(2020, 1, 1)
FREQUENCIES = ["1min", "2min", "15min", "1h", "5h", "1d"]

DURATION_MAP = {
    "1D": timedelta(days=1),
    "1M": timedelta(days=30),
    "1Y": timedelta(days=365),
    "3Y": timedelta(days=3 * 365),
}

OUTPUT_DIR = "output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

results = []

def benchmark(provider_cls, label):
    provider = provider_cls()
    print(f"\n=== {label} Benchmark ===")

    for freq, (duration_label, delta) in product(FREQUENCIES, DURATION_MAP.items()):
        case_name = f"{freq}-{duration_label}"
        start = START_DATE
        end = START_DATE + delta

        print(f"Priming cache for {case_name}...")
        provider.get(SYMBOL, start, end, freq)

        t0 = time.perf_counter()
        df = provider.get(SYMBOL, start, end, freq)
        t1 = time.perf_counter()

        elapsed = round(t1 - t0, 4)
        row_count = len(df)

        print(f"[{case_name}] {elapsed:.4f}s — Rows: {row_count}")
        results.append({
            "Format": label,
            "Range": case_name,
            "Time (s)": elapsed,
            "Rows": row_count
        })

if __name__ == "__main__":
    benchmark(PartitionedCSVBarsProvider, "Partitioned CSV")
    benchmark(ParquetBarsProvider, "Parquet")

    df = pd.DataFrame(results)

    # Create side-by-side comparison
    csv_df = df[df["Format"] == "Partitioned CSV"].set_index("Range")
    parquet_df = df[df["Format"] == "Parquet"].set_index("Range")

    side_by_side = csv_df[["Rows", "Time (s)"]].join(
        parquet_df[["Rows", "Time (s)"]],
        lsuffix=" CSV", rsuffix=" Parquet"
    ).reset_index()

    side_by_side.columns = [
        "Range",
        "Partitioned CSV Rows",
        "Partitioned CSV Time (s)",
        "Parquet Rows",
        "Parquet Time (s)"
    ]

    csv_path = os.path.join(OUTPUT_DIR, "benchmark_report.csv")
    side_by_side.to_csv(csv_path, index=False)
    print(f"\nSide-by-side benchmark results saved to: {csv_path}")
