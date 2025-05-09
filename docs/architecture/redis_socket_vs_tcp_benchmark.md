
# Redis Unix Socket vs TCP Access Benchmark Report (Target OS: Linux)

## Benchmark Comparison: TCP vs Unix Socket

### TCP (Before)
![TCP Access Benchmark]

- **AAPL:** 0.005229s
- **MSFT:** 0.003512s
- **GOOGL:** 0.003587s

### Unix Socket (After)
![Unix Socket Benchmark]

- **AAPL:** 0.001165s
- **MSFT:** 0.001416s
- **GOOGL:** 0.001473s

### Redis Benchmark Comparison Report
Benchmark Setup
Read/Write Coordination:

Reader: poetry run python -m fastscanner.benchmarks.benchmark_redis_read

Writer: poetry run python -m fastscanner.benchmarks.benchmark_polygon_realtime

System: Native Redis installation using UNIX socket

Client: Python with redis.asyncio

### Redis Configurations Tested:

appendfsync everysec (enabled)

appendfsync no (disabled persistence)


### Benchmark Results Summary


## Test 1: With appendfsync everysec
Metric	Result
Avg Messages/Batch	~3700–4100
Avg Latency	~0.000443s–0.000457s
Max Latency	~0.0050s–0.0090s
Avg Batch Duration	~12.0–13.5 seconds

Performance is stable, with <1ms average latency even with persistence enabled.

## Test 2: With appendfsync no (No Persistence)
Metric	Result
Avg Messages/Batch	~3700+
Avg Latency	Slightly lower (~0.0004s or better)
Max Latency	Slightly better (~0.005s)
Avg Batch Duration	Slightly shorter (~11.5–12.5s)

Marginally better performance

### Takeaways

appendfsync everysec offers a good trade-off between durability and performance.

The latency impact is minimal (~50–100 µs per op) even under 4000+ msg batches.