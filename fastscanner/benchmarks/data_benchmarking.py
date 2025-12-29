import asyncio
import os
import time
from datetime import date, timedelta
from itertools import product

import pandas as pd
import uvloop

from fastscanner.adapters.candle.partitioned_csv import PartitionedCSVCandlesProvider
from fastscanner.adapters.candle.polygon import PolygonCandlesProvider
from fastscanner.pkg import config
from fastscanner.services.indicators.ports import CandleStore

SYMBOL = "AAPL"
START_DATE = date(2020, 1, 5)
FREQUENCIES = ["1min", "2min", "15min", "1h", "5h", "1d"]

DURATION_MAP = {
    "1D": timedelta(days=1),
    "1M": timedelta(days=30),
    "3M": timedelta(days=90),
}

OUTPUT_DIR = "output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

results = []


async def benchmark(provider: CandleStore, label):
    print(f"\n=== {label} Benchmark ===")

    for freq, (duration_label, delta) in product(FREQUENCIES, DURATION_MAP.items()):
        case_name = f"{freq}-{duration_label}"
        start = START_DATE
        end = START_DATE + delta

        print(f"Priming cache for {case_name}...")
        df = await provider.get(SYMBOL, start, end, freq)

        n_samples = 100
        elapsed = 0
        for _ in range(n_samples):
            t0 = time.perf_counter()
            df = await provider.get(SYMBOL, start, end, freq)
            t1 = time.perf_counter()
            elapsed += t1 - t0

        elapsed = round(elapsed / n_samples, 4)
        row_count = len(df)

        print(f"[{case_name}] {elapsed:.4f}s — Rows: {row_count}")
        results.append(
            {
                "Format": label,
                "Range": case_name,
                "Time (s)": elapsed,
                "Rows": row_count,
                "Memory (MB)": round(df.memory_usage(deep=True).sum() / 1024**2, 2),
            }
        )


if __name__ == "__main__":

    async def main():
        polygon = PolygonCandlesProvider(
            config.POLYGON_BASE_URL, config.POLYGON_API_KEY
        )
        await benchmark(PartitionedCSVCandlesProvider(polygon), "Partitioned CSV")

        df = pd.DataFrame(results)

        # Create side-by-side comparison
        csv_df = df[df.loc[:, "Format"] == "Partitioned CSV"].set_index("Range")

        side_by_side = csv_df[["Rows", "Time (s)", "Memory (MB)"]].reset_index()

        side_by_side.columns = [
            "Range",
            "Partitioned CSV Rows",
            "Partitioned CSV Time (s)",
            "Partitioned CSV Memory (MB)",
        ]

        csv_path = os.path.join(OUTPUT_DIR, "benchmark_report.csv")
        side_by_side.to_csv(csv_path, index=False)
        print(f"\nSide-by-side benchmark results saved to: {csv_path}")

    uvloop.run(main())
