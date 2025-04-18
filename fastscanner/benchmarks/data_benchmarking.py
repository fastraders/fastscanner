import time
from datetime import date, timedelta
from itertools import product

from fastscanner.adapters.candle.partitioned_csv import PartitionedCSVBarsProvider
from fastscanner.adapters.candle.parquet import ParquetBarsProvider
from fastscanner.adapters.candle.partitioned_parquet import PartitionedParquetBarsProvider

SYMBOL = "AAPL"
START_DATE = date(2020, 1, 1)

FREQUENCIES = ["1min", "2min", "15min", "1h", "5h", "1d"]

DURATION_MAP = {
    "1D": timedelta(days=1),
    "1M": timedelta(days=30),
    "1Y": timedelta(days=365),
    "3Y": timedelta(days=3 * 365),
}

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

        print(f"[{case_name}] {t1 - t0:.4f}s — Rows: {len(df)}")

if __name__ == "__main__":
    benchmark(PartitionedCSVBarsProvider, "Partitioned CSV")
    benchmark(ParquetBarsProvider, "Parquet")
