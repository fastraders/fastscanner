import time
from datetime import date
from fastscanner.adapters.candle.partitioned_csv import PartitionedCSVBarsProvider
from fastscanner.adapters.candle.parquet import ParquetBarsProvider
from fastscanner.adapters.candle.partitioned_parquet import PartitionedParquetBarsProvider

SYMBOL = "AAPL"
FREQ = "1h"

RANGES = {
    "Start (Jan)": (date(2013, 1, 1), date(2023, 1, 31)),
    "Mid (May)":   (date(2014, 5, 1), date(2023, 5, 31)),
    "End (Sep)":   (date(2016, 9, 1), date(2023, 9, 30)),
}

def benchmark(provider_cls, label):
    provider = provider_cls()
    print(f"\nBenchmarking {label}")

    print("Priming full range...")
    provider.get(SYMBOL, date(2013, 1, 1), date(2023, 10, 1), FREQ)

    for desc, (start_date, end_date) in RANGES.items():
        t0 = time.perf_counter()
        df = provider.get(SYMBOL, start_date, end_date, FREQ)
        t1 = time.perf_counter()
        print(f"[{desc}] {label}: {t1 - t0:.4f}s — Rows: {len(df)}")


if __name__ == "__main__":
    benchmark(PartitionedCSVBarsProvider, "Partitioned CSV")
    benchmark(ParquetBarsProvider, "Parquet")
    benchmark(PartitionedParquetBarsProvider, "Partitioned Parquet")
