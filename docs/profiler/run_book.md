# FastScanner Profiling Runbook (Austin Web)

## How do I profile a module in my code that could be creating performance issues?

If you suspect a specific module or function in your code is causing performance issues, follow these steps:

1. **Identify the entry point** (script, function, or module) you want to profile.
2. **Run Austin Web** with your entry point:
   ```bash
   austin-web -S python -m <your.module> [args...]
   ```
   or
   ```bash
   austin-web -S python path/to/your_script.py [args...]
   ```
3. **Open the Austin Web interface** (URL will be shown in the terminal).
4. **Analyze the flame graph:**
   - Focus on wide blocks under your own code (not system/runner functions).
   - Identify which functions or lines are taking the most time.
5. **Optimize the slowest parts** and re-profile to measure improvements.

*Tip: If your module is only used as part of a larger workflow, create a minimal script that exercises just the part you want to profile.*

## Purpose
This runbook provides a clear, step-by-step guide for profiling FastScanner's batch and realtime scanners using the Austin Web profiler. It is focused on actionable steps and best practices for future performance analysis.

## Prerequisites
- Austin profiler installed and available in PATH

## Step-by-Step Profiling Guide

### 1. Batch Scanner Profiling (benchmark_scanner)
1. **Start profiling with Austin Web:**
   ```bash
   make austin-web-scanner
   ```
2. **Open the Austin Web interface:**
   - The URL will be shown in the terminal (usually http://localhost:5000).
3. **Run the scanner as usual.**
4. **Analyze the flame graph:**
   - Focus on wide blocks under your own code (e.g., `read_csv`, indicator functions).
   - Ignore top-level system/runner functions (e.g., `run`, event loop).
   - Look for bottlenecks in file I/O and indicator calculations.

### 2. Realtime Scanner Profiling (benchmark_scanner_realtime)
1. **Start profiling with Austin Web:**
   ```bash
   make austin-web-scanner-realtime
   ```
2. **Open the Austin Web interface:**
   - The URL will be shown in the terminal.
3. **Run the realtime scanner as usual.**
4. **Analyze the flame graph:**
   - Focus on wide blocks under your own code (e.g., Redis stream reading, event handlers, indicator functions).
   - Ignore system/runner functions.
   - Look for bottlenecks in Redis I/O, event processing, and indicator calculations.

## Best Practices
- **Re-profile after making optimizations to measure impact.**

## Troubleshooting
- If Austin Web does not start, ensure Austin is installed and available in your PATH.
- If the flame graph is dominated by system functions, zoom in on your own code blocks for actionable insights.

---

*For detailed analysis and conclusions, see the profiling analysis report.*
