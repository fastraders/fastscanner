import time
from datetime import date, timedelta
from fastscanner.adapters.candle.partitioned_csv import PartitionedCSVBarsProvider
from fastscanner.adapters.candle.parquet import ParquetBarsProvider

SYMBOL = "AAPL"

# Frequency and duration pairs
TEST_CASES = {
    "1min-1D":  ("1min", timedelta(days=1)),
    "2min-1M":  ("2min", timedelta(days=30)),
    "15min-1Y": ("15min", timedelta(days=365)),
    "1h-3Y":    ("1h", timedelta(days=365 * 3)),
    "5h":       ("5h", timedelta(days=180)),  # Approx 6 months (fallback default)
    "1d":       ("1d", timedelta(days=365)),  # 1 year
}

START_DATE = date(2020, 1, 1)  # Base start date

def benchmark(provider_cls, label):
    provider = provider_cls()
    print(f"\n=== {label} Benchmark ===")

    for case_name, (freq, duration) in TEST_CASES.items():
        start = START_DATE
        end = START_DATE + duration

        # Prime cache
        print(f"Priming cache for {case_name}...")
        provider.get(SYMBOL, start, end, freq)

        # Benchmark
        t0 = time.perf_counter()
        df = provider.get(SYMBOL, start, end, freq)
        t1 = time.perf_counter()

        print(f"[{case_name}] {t1 - t0:.4f}s — Rows: {len(df)}")

if __name__ == "__main__":
    benchmark(PartitionedCSVBarsProvider, "Partitioned CSV")
    benchmark(ParquetBarsProvider, "Parquet")
