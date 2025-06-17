# FastScanner Austin Profiling Run Book

## Overview

FastScanner uses Austin profiler for performance analysis.

## Prerequisites

- Austin profiler installed and available in PATH


## Quick Start

### 1. View Available Commands
```bash
make austin-help
```

### 2. Basic Profiling
```bash
# Profile benchmark scanner with web interface
make austin-web-scanner

# Profile realtime scanner with terminal interface
make austin-tui-scanner-realtime
```

## Makefile Commands

### Direct Commands (Recommended)

#### Austin Web (Browser-based visualization)
```bash
# Profile benchmark scanner - opens web interface automatically
make austin-web-scanner

# Profile realtime benchmark scanner - opens web interface automatically  
make austin-web-scanner-realtime
```

#### Austin TUI (Terminal-based visualization)
```bash
# Profile benchmark scanner - real-time terminal visualization
make austin-tui-scanner

# Profile realtime benchmark scanner - real-time terminal visualization
make austin-tui-scanner-realtime
```

## Direct Austin Commands

If you prefer to run Austin directly without Make:

### Austin Web
```bash
# Basic web profiling
austin-web -S python -m fastscanner.benchmarks.benchmark_scanner

# Web profiling with time limit (60 seconds)
austin-web -S -x 60 python -m fastscanner.benchmarks.benchmark_scanner_realtime

# Web profiling with memory tracking
austin-web -S -m python -m fastscanner.benchmarks.benchmark_scanner
```

### Austin TUI
```bash
# Basic TUI profiling
austin-tui python -m fastscanner.benchmarks.benchmark_scanner

# TUI profiling with memory tracking
austin-tui -m python -m fastscanner.benchmarks.benchmark_scanner_realtime

# TUI profiling with high-resolution sampling
austin-tui -i 1ms python -m fastscanner.benchmarks.benchmark_scanner
```

### Austin CLI (Save to files)
```bash
# Save profile to file
austin -o output/profiling/profile.austin -x 30 python -m fastscanner.benchmarks.benchmark_scanner

# Save with memory profiling
austin -m -o output/profiling/profile_memory.austin -x 30 python -m fastscanner.benchmarks.benchmark_scanner

# Save with full metrics (time + memory)
austin -f -o output/profiling/profile_full.austin -x 30 python -m fastscanner.benchmarks.benchmark_scanner
```

## Austin Options Explained

### Common Flags
- **`-S`** - Start web server (required for austin-web)
- **`-x 60`** - Profile for 60 seconds only
- **`-m`** - Enable memory profiling
- **`-f`** - Full metrics (time + memory)
- **`-i 1ms`** - Sampling interval (default: 100μs)
- **`-o file.austin`** - Save output to file

### Profile Types

#### 1. Time Profiling (Default)
Shows CPU time spent in each function
```bash
austin-web -S python -m fastscanner.benchmarks.benchmark_scanner
```

#### 2. Memory Profiling
Shows memory allocations and deallocations
```bash
austin-web -S -m python -m fastscanner.benchmarks.benchmark_scanner
```

#### 3. Full Profiling
Shows both time and memory metrics
```bash
austin-web -S -f python -m fastscanner.benchmarks.benchmark_scanner
```

## Useful Commands Reference

### Quick Commands
| Command | Description |
|---------|-------------|
| `make austin-help` | Show all available commands |
| `make austin-web-scanner` | Quick web profiling of benchmark scanner |
| `make austin-tui-scanner-realtime` | Quick TUI profiling of realtime scanner |
| `make austin-web-benchmarks` | Interactive benchmark selection |

### Direct Austin Commands
| Command | Description |
|---------|-------------|
| `austin-web -S <script>` | Web interface profiling |
| `austin-tui <script>` | Terminal interface profiling |
| `austin -o file.austin <script>` | Save profile to file |
| `austin --help` | Show all austin options |
