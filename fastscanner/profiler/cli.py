import argparse
import logging
import sys
from pathlib import Path

from fastscanner.pkg.logging import load_logging_config

from .profiler import AustinProfiler

load_logging_config()
logger = logging.getLogger(__name__)


def profile_script(args):
    profiler = AustinProfiler(
        output_dir=args.output_dir,
        sampling_interval=args.interval,
        mode=args.mode,
        timeout=args.timeout,
    )

    result = profiler.profile(args.script, output_name=args.output_name)
    logger.info(f"Profiling completed: {result}")


def profile_realtime(args):
    profiler = AustinProfiler(
        output_dir=args.output_dir,
        sampling_interval=args.interval,
        mode=args.mode,
        timeout=args.duration,
    )

    if args.duration:
        logger.info(f"Profiling realtime script for {args.duration} seconds...")
        logger.info("This will capture multiple batches/messages during that time.")

    result = profiler.profile(args.script, output_name=args.output_name)
    logger.info(f"Realtime profiling completed: {result}")

    if result:
        summary = profiler.get_profiling_summary(result)
        logger.info(
            f"Captured {summary['total_samples']} samples over {args.duration}s"
        )
        logger.info(f"Profile size: {summary['file_size_kb']} KB")


def profile_benchmarks(args):
    profiler = AustinProfiler(
        output_dir=args.output_dir,
        sampling_interval=args.interval,
        mode=args.mode,
        timeout=args.timeout,
    )

    benchmark_scripts = [
        "fastscanner.benchmarks.benchmark_scanner",
        "fastscanner.benchmarks.benchmark_scanner_realtime",
        "fastscanner.benchmarks.benchmark_realtime_subscribe",
        "fastscanner.benchmarks.data_benchmarking",
        "fastscanner.benchmarks.benchmark_polygon_realtime",
        "fastscanner.benchmarks.benchmark_redis_read",
        "fastscanner.benchmarks.calculate_indicators",
    ]

    results = profiler.profile_multiple(benchmark_scripts)

    successful = len([r for r in results.values() if r is not None])
    logger.info(f"Profiled {successful}/{len(results)} benchmark scripts")

    for script_name, profile_path in results.items():
        if profile_path:
            logger.info(f"   {script_name}: {profile_path}")
        else:
            logger.info(f"   {script_name}: FAILED")


def profile_cmd_scripts(args):
    profiler = AustinProfiler(
        output_dir=args.output_dir,
        sampling_interval=args.interval,
        mode=args.mode,
        timeout=args.timeout,
    )

    cmd_scripts = [
        "fastscanner.adapters.cmd.run_scanner",
        "fastscanner.adapters.cmd.run_candles_collect",
        "fastscanner.adapters.cmd.run_data_collection",
        "fastscanner.adapters.cmd.run_eodhd_collect",
        "fastscanner.adapters.cmd.run_data_provider",
        "fastscanner.adapters.cmd.run_realtime_provider",
        "fastscanner.adapters.cmd.run_eodhd_provider",
    ]

    results = profiler.profile_multiple(cmd_scripts)

    successful = len([r for r in results.values() if r is not None])
    logger.info(f"Profiled {successful}/{len(cmd_scripts)} cmd scripts")

    for script_name, profile_path in results.items():
        if profile_path:
            logger.info(f"   {script_name}: {profile_path}")
        else:
            logger.info(f"   {script_name}: FAILED")


def profile_multiple_scripts(args):
    profiler = AustinProfiler(
        output_dir=args.output_dir,
        sampling_interval=args.interval,
        mode=args.mode,
        timeout=args.timeout,
    )

    scripts = args.scripts.split(",")
    results = profiler.profile_multiple(scripts)

    successful = len([r for r in results.values() if r is not None])
    logger.info(f"Profiled {successful}/{len(scripts)} scripts")

    for script_name, profile_path in results.items():
        if profile_path:
            logger.info(f"   {script_name}: {profile_path}")
        else:
            logger.info(f"   {script_name}: FAILED")


def main():
    parser = argparse.ArgumentParser(
        description="FastScanner Austin Profiler CLI - Simple profiling for any script",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Profile a single script
  poetry run python -m fastscanner.profiler.cli script fastscanner.benchmarks.benchmark_scanner
  
  # Profile realtime script for 30 seconds (captures multiple batches)
  poetry run python -m fastscanner.profiler.cli realtime fastscanner.benchmarks.benchmark_scanner_realtime --duration 30
  
  # Profile realtime with memory tracking for 60 seconds
  poetry run python -m fastscanner.profiler.cli realtime benchmark_scanner_realtime --mode memory --duration 60
  
  # Profile all benchmarks
  poetry run python -m fastscanner.profiler.cli benchmarks
  
  # Profile all cmd scripts  
  poetry run python -m fastscanner.profiler.cli cmd-scripts
  
  # Profile multiple custom scripts
  poetry run python -m fastscanner.profiler.cli multiple "script1,script2,script3"
  
  # Profile with different settings
  poetry run python -m fastscanner.profiler.cli script benchmark_scanner --mode memory --interval 500
        """,
    )

    parser.add_argument(
        "--output-dir", default="output/profiling", help="Output directory for profiles"
    )
    parser.add_argument(
        "--mode",
        choices=["wall", "cpu", "memory"],
        default="wall",
        help="Profiling mode",
    )
    parser.add_argument(
        "--interval", type=int, default=1000, help="Sampling interval in microseconds"
    )
    parser.add_argument("--timeout", type=int, help="Profiling timeout in seconds")

    subparsers = parser.add_subparsers(dest="command", help="Commands")

    script_parser = subparsers.add_parser("script", help="Profile a single script")
    script_parser.add_argument(
        "script",
        help="Script module path (e.g., fastscanner.benchmarks.benchmark_scanner)",
    )
    script_parser.add_argument("--output-name", help="Custom output filename")
    script_parser.set_defaults(func=profile_script)

    realtime_parser = subparsers.add_parser(
        "realtime", help="Profile realtime/streaming scripts"
    )
    realtime_parser.add_argument(
        "script",
        help="Realtime script module path (e.g., fastscanner.benchmarks.benchmark_scanner_realtime)",
    )
    realtime_parser.add_argument(
        "--duration",
        type=int,
        required=True,
        help="How long to profile in seconds (captures multiple batches)",
    )
    realtime_parser.add_argument("--output-name", help="Custom output filename")
    realtime_parser.set_defaults(func=profile_realtime)

    benchmarks_parser = subparsers.add_parser(
        "benchmarks", help="Profile all benchmark scripts"
    )
    benchmarks_parser.set_defaults(func=profile_benchmarks)

    cmd_scripts_parser = subparsers.add_parser(
        "cmd-scripts", help="Profile all cmd scripts"
    )
    cmd_scripts_parser.set_defaults(func=profile_cmd_scripts)

    multiple_parser = subparsers.add_parser(
        "multiple", help="Profile multiple custom scripts"
    )
    multiple_parser.add_argument(
        "scripts",
        help="Comma-separated list of script module paths",
    )
    multiple_parser.set_defaults(func=profile_multiple_scripts)

    args = parser.parse_args()

    if not args.command:
        return

    logging.getLogger().setLevel(logging.DEBUG)

    try:
        args.func(args)
    except Exception as e:
        logger.error(f"Profiling failed: {e}")
        import traceback

        sys.exit(1)


if __name__ == "__main__":
    main()


# # Profile the benchmark_scanner script specifically
# poetry run python -m fastscanner.profiler.cli script fastscanner.benchmarks.benchmark_scanner

# Profile all benchmark scripts at once
# poetry run python -m fastscanner.profiler.cli benchmarks

# Profile with memory profiling and higher sampling rate
# poetry run python -m fastscanner.profiler.cli script fastscanner.benchmarks.benchmark_scanner \
#     --mode memory \
#     --interval 500 \
#     --output-dir my_profiles \
#     --timeout 300

# # Profile with CPU profiling
# poetry run python -m fastscanner.profiler.cli script fastscanner.benchmarks.benchmark_scanner \
#     --mode cpu \
#     --interval 1000
