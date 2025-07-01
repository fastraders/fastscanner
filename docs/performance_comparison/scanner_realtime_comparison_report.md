
# Scanner Realtime Performance Comparison Report

## Overview

This report evaluates the performance of two different implementations of the `scanner_realtime` logic:

- **Dict-based implementation:** Returns a `dict[str, Any]` from the indicator.
- **Series-based implementation:** Returns a `pd.Series` from the indicator.

Both were benchmarked with **1,000 symbols**, using **1-minute frequency**. The performance was logged for each period with metrics such as read/write latency, total messages processed, and derived latencies.

---

## Key Metrics Compared

| Metric                    | Dict Implementation       | Series Implementation     |
|---------------------------|---------------------------|---------------------------|
| **Average Read Latency**  | Lower                     | Higher                    |
| **Average Write Latency** | Negligible (~0.0007s)     | Negligible (~0.0007s)     |
| **Total Messages Read**   | Slightly higher           | Slightly lower            |
| **Latency per Message**   | Significantly lower       | Higher                    |

> The `dict` implementation consistently showed lower read latency and better efficiency per message.

---

## Visual Comparison (Summary)

### 1. Read Latency (ms)
- Dict: Peaks mostly under 5ms.
- Series: Peaks range up to 70ms or more.

### 2. Latency per Message
- Dict: ~0.003s to ~0.065s
- Series: ~0.01s to ~0.17s

### 3. Read vs Write Gap
- Dict implementation shows smaller gaps, ensuring faster processing.

---

## Results Snapshot

Below are the first 10 rows of recorded data used for analysis:

```csv
datetime,write_latency,read_latency,total_messages_read,write_latency.1,total_messages_written,read_minus_write_latency,latency_per_message,implementation
2025-07-01 19:23:23,0.0016,74.3994,1144,0.0016,1380,74.3979,0.065,dict
2025-07-01 19:24:17,0.0008,4.7701,545,0.0008,2895,4.7693,0.0088,dict
2025-07-01 19:25:12,0.0008,3.079,533,0.0008,2808,3.0782,0.0058,dict
2025-07-01 19:26:11,0.0006,2.0835,528,0.0006,2742,2.0829,0.0039,dict
2025-07-01 19:27:13,0.0006,2.0304,541,0.0006,2843,2.0298,0.0038,dict
2025-07-01 19:28:11,0.0005,1.7475,532,0.0005,2781,1.747,0.0033,dict
2025-07-01 19:29:10,0.0004,1.2134,551,0.0004,2900,1.213,0.0022,dict
2025-07-01 19:30:12,0.0005,1.0079,519,0.0005,2743,1.0075,0.0019,dict
2025-07-01 19:31:16,0.0004,7.2987,547,0.0004,2861,7.2983,0.0133,dict
2025-07-01 19:32:14,0.0004,2.3141,545,0.0004,2863,2.3137,0.0042,dict

```

---

## Conclusion

- **Dict-based implementation is more performant**, particularly in high-frequency, high-symbol-count scenarios.
- Avoiding the overhead of `pd.Series` improves latency and throughput.

---

