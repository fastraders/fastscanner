"""Example usage of the CandleSubscriptionClient."""

import asyncio

from fastscanner.client import (
    ATR,
    CandleSubscriptionClient,
    CumulativeDailyVolume,
    DailyGap,
    PositionInRange,
)
from fastscanner.pkg.logging import load_logging_config


def handle_candle_data(message):
    """Handle incoming candle data with indicators."""
    print(f"Received data for {message.symbol} at {message.timestamp}")
    print(f"Candle: {message.candle}")
    print("---")


async def main():
    """Example of using the client to subscribe to multiple indicators."""

    load_logging_config()
    # Create client
    client = CandleSubscriptionClient("ws://localhost:12356/api/indicators")

    try:
        # Connect to websocket
        await client.connect()

        # Define indicators
        indicators = [
            ATR(period=14, freq="3min"),
            DailyGap(),
            CumulativeDailyVolume(),
            PositionInRange(n_days=5),
        ]

        # Subscribe to AAPL with indicators
        for symbol in ["AAPL", "MSFT", "GOOGL", "AMZN", "ABNB"]:
            await client.subscribe(
                subscription_id=f"{symbol.lower()}_indicators",
                symbol=symbol,
                freq="3min",
                indicators=indicators,
                callback=handle_candle_data,
            )
        print("Subscribed to AAPL indicators. Listening for data...")

        # Listen for messages
        await client.listen()

    except KeyboardInterrupt:
        print("Stopping...")
    finally:
        await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
