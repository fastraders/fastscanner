
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
| **Latency per Message**   |  lower                    | Higher                    |

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
datetime,write latency,read latency,total messages read,write latency,total messages written,read minus write latency,latency per message
2025-07-01 22:01:03.935,0.000982,33.175049,560,0.000982,2909,33.174067,0.059241
2025-07-01 22:02:00.794,0.000750,6.691891,554,0.000750,202,6.691141,0.012079
2025-07-01 22:02:10.795,0.001018,6.691891,554,0.001018,2881,6.690873,0.012079
2025-07-01 22:03:06.747,0.001547,5.267684,523,0.001547,2749,5.266137,0.010072
2025-07-01 22:03:58.756,0.001258,3.578905,483,0.001258,510,3.577647,0.007410
2025-07-01 22:05:04.750,0.001147,3.015549,490,0.001147,2598,3.014402,0.006154
2025-07-01 22:06:01.315,0.001200,4.374803,569,0.001200,1005,4.373603,0.007688
2025-07-01 22:06:11.316,0.001599,4.374803,569,0.001599,2926,4.373204,0.007688
2025-07-01 22:07:07.643,0.001635,5.951190,515,0.001635,2675,5.949555,0.011556
2025-07-01 22:08:00.687,0.001302,1.849479,486,0.001302,2437,1.848177,0.003805
2025-07-01 22:09:07.304,0.001161,3.193693,533,0.001161,2661,3.192532,0.005992
2025-07-01 22:10:03.093,0.000924,1.720506,490,0.000924,2592,1.719582,0.003511
2025-07-01 22:11:06.010,0.001142,2.052934,520,0.001142,2629,2.051792,0.003948
2025-07-01 22:12:01.835,0.001299,1.900097,555,0.001299,2876,1.898798,0.003423
2025-07-01 22:13:07.650,0.001609,2.465038,540,0.001609,2849,2.463429,0.004565
2025-07-01 22:14:00.562,0.000973,1.494858,547,0.000973,633,1.493885,0.002733

```

---

## Conclusion

- **Dict-based implementation is more performant**, particularly in high-frequency, high-symbol-count scenarios.
- Avoiding the overhead of `pd.Series` improves latency and throughput.

---

