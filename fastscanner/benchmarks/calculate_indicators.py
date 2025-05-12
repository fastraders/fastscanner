import argparse
import asyncio
import json
import logging
import multiprocessing
import os
import random
import time
from datetime import date, datetime, timedelta
from itertools import islice
from typing import Any, Dict, List, Optional, Set, Tuple

import httpx
import numpy as np
import pandas as pd

from fastscanner.adapters.candle.polygon import PolygonCandlesProvider
from fastscanner.pkg import config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Constants
DEFAULT_REQUESTS_FILE = "output/benchmark_requests.json"
DEFAULT_STATS_FILE = "output/benchmark_stats.json"
DEFAULT_NUM_REQUESTS = 10000
REQUEST_BATCH_SIZE = 100
DEFAULT_CONCURRENCY = 2 * (multiprocessing.cpu_count() * 2 + 1)
DEFAULT_TIMEOUT = 30  # seconds

# Indicator parameter ranges
INDICATOR_PARAM_RANGES = {
    "cumulative_daily_volume": {},
    "premarket_cumulative": {
        "candle_col": ["open", "high", "low", "close", "volume"],
        "op": ["min", "max", "sum"],
    },
    "atr": {
        "period": (5, 30),  # (min, max)
    },
    "position_in_range": {
        "n_days": (5, 30),  # (min, max)
    },
    "prev_day": {
        "candle_col": ["open", "high", "low", "close", "volume"],
    },
    "daily_gap": {},
    "daily_atr": {
        "period": (5, 30),  # (min, max)
    },
    "daily_atr_gap": {
        "period": (5, 30),  # (min, max)
    },
    "days_to_earnings": {},
    "days_from_earnings": {},
}


def get_random_indicator_params(indicator_type: str) -> Dict[str, Any]:
    """Generate random parameters for a given indicator type."""
    params = {}
    param_ranges = INDICATOR_PARAM_RANGES.get(indicator_type, {})

    for param_name, param_range in param_ranges.items():
        if isinstance(param_range, list):
            params[param_name] = random.choice(param_range)
        elif isinstance(param_range, tuple) and len(param_range) == 2:
            min_val, max_val = param_range
            params[param_name] = random.randint(min_val, max_val)

    return params


def generate_request_body(
    symbol: str, start_date: date, end_date: date, freq: str, num_indicators: int = 3
) -> Dict[str, Any]:
    """Generate a random request body for the /calculate endpoint."""
    # Select random indicator types
    available_indicators = list(INDICATOR_PARAM_RANGES.keys())
    selected_indicators = random.sample(
        available_indicators, min(num_indicators, len(available_indicators))
    )

    indicators = []
    for indicator_type in selected_indicators:
        params = get_random_indicator_params(indicator_type)
        if indicator_type == "atr":
            params["freq"] = freq
        indicators.append({"type": indicator_type, "params": params})

    return {
        "symbol": symbol,
        "start": start_date.isoformat(),
        "end": end_date.isoformat(),
        "freq": freq,
        "indicators": indicators,
    }


def generate_requests(
    symbols: Set[str], num_requests: int, requests_file: str
) -> List[Dict[str, Any]]:
    """Generate request bodies and save them to a file."""
    if os.path.exists(requests_file):
        logger.info(f"Loading existing requests from {requests_file}")
        with open(requests_file, "r") as f:
            return json.load(f)

    logger.info(f"Generating {num_requests} random requests")

    # Convert symbols set to list for random sampling
    symbols_list = list(symbols)

    # Generate random date ranges (last 1-2 years)
    end_date = date.today()

    requests = []
    for _ in range(num_requests):
        # Random symbol
        symbol = random.choice(symbols_list)

        # Random date range (1-60 days in the last 2 years)
        days_back = random.randint(1, 10 * 365)  # Up to 2 years back
        range_days = random.randint(1, 60)  # 1-60 day range

        start_date = end_date - timedelta(days=days_back)
        req_end_date = start_date + timedelta(days=range_days)

        # Random frequency
        freq = random.choice(["1min", "5min", "15min", "30min", "1h"])

        # Generate 1-5 random indicators
        num_indicators = random.randint(1, 5)

        request = generate_request_body(
            # Generate request body
            symbol,
            start_date,
            req_end_date,
            freq,
            num_indicators,
        )
        requests.append(request)

    # Save requests to file
    with open(requests_file, "w") as f:
        json.dump(requests, f, indent=2)

    logger.info(f"Saved {len(requests)} requests to {requests_file}")
    return requests


async def send_requests(
    client: httpx.AsyncClient,
    base_url: str,
    request_body: list[Dict[str, Any]],
    timeout: int,
) -> Tuple[list[Dict[str, Any]], Optional[str], float]:
    """Send a request to the /calculate endpoint and return the result."""
    url = f"{base_url}/indicators/calculate"
    start_time = time.perf_counter()

    try:
        response = await client.post(url, json=request_body, timeout=timeout)
        elapsed = time.perf_counter() - start_time

        if response.status_code == 200:
            return request_body, response.json(), elapsed
        else:
            logger.error(
                f"Request failed with status {response.status_code}: {response.text}"
            )
            return request_body, None, elapsed
    except Exception as e:
        elapsed = time.perf_counter() - start_time
        logger.error(f"Request failed with exception: {str(e)}")
        return request_body, None, elapsed


async def calculate_indicators(
    requests: List[Dict[str, Any]],
    base_url: str,
    concurrency: int,
    timeout: int,
    stats_file: str,
) -> Dict[str, Any]:
    """Run the stress test with the given requests."""

    logger.info(
        f"Running stress test with {len(requests)} requests, concurrency={concurrency}"
    )

    results = []
    stats = {
        "total_requests": len(requests),
        "successful_requests": 0,
        "failed_requests": 0,
        "avg_response_time": 0,
        "min_response_time": float("inf"),
        "max_response_time": 0,
        "symbol_stats": {},
    }

    # Use a semaphore to limit concurrency
    semaphore = asyncio.Semaphore(concurrency)
    completed_count = 0
    total_count = len(requests) // REQUEST_BATCH_SIZE

    async def process_request(client, requests):
        nonlocal completed_count
        async with semaphore:
            result = await send_requests(client, base_url, requests, timeout)
            completed_count += 1
            if completed_count % 10 == 0 or completed_count == total_count:
                logger.info(f"Completed {completed_count}/{total_count} requests")
            return result

    # Create tasks for all requests but limit concurrency with semaphore
    async with httpx.AsyncClient() as client:
        tasks = [
            process_request(client, requests[i : i + REQUEST_BATCH_SIZE])
            for i in range(0, len(requests), REQUEST_BATCH_SIZE)
        ]
        batch_results = await asyncio.gather(*tasks)
        results.extend(batch_results)

    # Process results and update stats
    total_time = 0
    for requests, responses, elapsed in results:
        if responses is None:
            stats["failed_requests"] += 1
            continue
        else:
            stats["successful_requests"] += 1

        total_time += elapsed
        stats["min_response_time"] = min(stats["min_response_time"], elapsed)
        stats["max_response_time"] = max(stats["max_response_time"], elapsed)
        for request, response in zip(requests, responses):
            symbol = request["symbol"]

            # Initialize symbol stats if not exists
            if symbol not in stats["symbol_stats"]:
                stats["symbol_stats"][symbol] = {
                    "total_requests": 0,
                    "successful_requests": 0,
                    "failed_requests": 0,
                    "avg_response_time": 0,
                }

            # Update symbol stats
            stats["symbol_stats"][symbol]["total_requests"] += 1
            symbol_total_time = (
                stats["symbol_stats"][symbol].get("total_time", 0) + elapsed
            )
            stats["symbol_stats"][symbol]["total_time"] = symbol_total_time

    # Calculate averages
    if stats["successful_requests"] > 0:
        stats["avg_response_time"] = total_time / len(results) / DEFAULT_CONCURRENCY

    for symbol, symbol_stat in stats["symbol_stats"].items():
        if symbol_stat["total_requests"] > 0:
            symbol_stat["avg_response_time"] = (
                symbol_stat["total_time"] / symbol_stat["total_requests"]
            )
            # Remove the total_time field as it's not needed in the final stats
            symbol_stat.pop("total_time", None)

    # If min_response_time is still infinity, set it to 0
    if stats["min_response_time"] == float("inf"):
        stats["min_response_time"] = 0

    # Save stats to file
    with open(stats_file, "w") as f:
        json.dump(stats, f, indent=2)

    logger.info(f"Stress test completed. Results saved to {stats_file}")
    logger.info(
        f"Success rate: {stats['successful_requests']}/{stats['total_requests']} "
        f"({stats['successful_requests']/stats['total_requests']*100:.2f}%)"
    )
    logger.info(f"Average response time: {stats['avg_response_time']:.4f}s")

    return stats


def get_polygon_symbols() -> Set[str]:
    """Get available symbols from Polygon."""
    logger.info("Fetching symbols from Polygon")

    # Create PolygonCandlesProvider
    provider = PolygonCandlesProvider(config.POLYGON_BASE_URL, config.POLYGON_API_KEY)

    # Get all symbols
    symbols = provider.all_symbols()
    logger.info(f"Found {len(symbols)} symbols from Polygon")

    return symbols


def main():
    """Main function to run the stress test."""
    parser = argparse.ArgumentParser(description="Stress test the /calculate endpoint")
    parser.add_argument(
        "--requests-file",
        type=str,
        default=DEFAULT_REQUESTS_FILE,
        help=f"Path to the requests file (default: {DEFAULT_REQUESTS_FILE})",
    )
    parser.add_argument(
        "--stats-file",
        type=str,
        default=DEFAULT_STATS_FILE,
        help=f"Path to the stats file (default: {DEFAULT_STATS_FILE})",
    )
    parser.add_argument(
        "--num-requests",
        type=int,
        default=DEFAULT_NUM_REQUESTS,
        help=f"Number of requests to generate (default: {DEFAULT_NUM_REQUESTS})",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=DEFAULT_CONCURRENCY,
        help=f"Number of concurrent requests (default: {DEFAULT_CONCURRENCY})",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=DEFAULT_TIMEOUT,
        help=f"Request timeout in seconds (default: {DEFAULT_TIMEOUT})",
    )
    parser.add_argument(
        "--base-url",
        type=str,
        default=f"http://localhost:{config.SERVER_PORT}/api",
        help=f"Base URL for the API (default: http://localhost:{config.SERVER_PORT}/api)",
    )

    args = parser.parse_args()

    # Get symbols from Polygon
    symbols = get_polygon_symbols()

    # Generate requests
    requests = generate_requests(symbols, args.num_requests, args.requests_file)

    # Run stress test

    asyncio.run(
        calculate_indicators(
            requests, args.base_url, args.concurrency, args.timeout, args.stats_file
        )
    )


if __name__ == "__main__":
    main()
