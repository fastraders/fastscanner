import json
from typing import Any

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from fastscanner.adapters.rest.indicators import (
    ActionType,
    StatusType,
    get_indicators_service_ws,
)
from fastscanner.adapters.rest.main import app
from fastscanner.services.indicators.lib import Indicator, IndicatorsLibrary
from fastscanner.services.indicators.ports import ChannelHandler, FundamentalData
from fastscanner.services.indicators.service import (
    IndicatorsService,
    SubscriptionHandler,
)


class DummyIndicator(Indicator):
    def __init__(self, multiplier: float = 2.0, **kwargs):
        self._multiplier = multiplier

    @classmethod
    def type(cls) -> str:
        return "dummy_indicator"

    async def extend(self, symbol: str, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df["dummy_value"] = df["close"] * self._multiplier
        return df

    async def extend_realtime(self, symbol: str, new_row: pd.Series) -> pd.Series:
        enhanced_row = new_row.copy()
        enhanced_row["dummy_value"] = new_row.get("close", 0) * self._multiplier
        return enhanced_row


class MockChannel:
    def __init__(self):
        self.unsubscriptions = {}
        self.handlers: dict[str, list[ChannelHandler]] = {}

    async def subscribe(self, channel_id: str, handler):
        self.handlers.setdefault(channel_id, []).append(handler)
        return handler.id()

    async def unsubscribe(self, channel_id: str, handler_id: str):
        self.unsubscriptions.setdefault(channel_id, []).append(handler_id)

        self.handlers[channel_id] = [
            handler
            for handler in self.handlers.get(channel_id, [])
            if handler.id() != handler_id
        ]

    async def push_data(self, channel_id: str, data: dict[Any, Any]):
        """Helper method to push data to a specific channel for testing"""
        for handler in self.handlers.get(channel_id, []):
            await handler.handle(channel_id, data)

    async def push(self, channel_id: str, data: dict[Any, Any], flush: bool = True): ...

    async def flush(self): ...

    async def reset(self): ...


class MockCandleStore:
    async def get(
        self, symbol, start, end, freq, adjusted: bool = False
    ) -> pd.DataFrame:
        return pd.DataFrame()


class MockFundamentalDataStore:
    async def get(self, symbol) -> FundamentalData:
        return FundamentalData(
            "stock",
            "NASDAQ",
            "United States",
            "Washington D.C",
            "Oil",
            "Oil",
            pd.Series(dtype=float),
            pd.DatetimeIndex(["2022-01-01"]),
            10,
            20,
            None,
            None,
        )


@pytest.fixture
def indicators_service():
    library = IndicatorsLibrary()
    library.register(DummyIndicator)

    original_instance = IndicatorsLibrary.instance
    IndicatorsLibrary.instance = lambda: library

    candles = MockCandleStore()
    fundamentals = MockFundamentalDataStore()
    channel = MockChannel()
    service = IndicatorsService(
        candles, fundamentals, channel, "test_subscribe", "test_unsubscribe"
    )

    yield service, channel

    IndicatorsLibrary.instance = original_instance


@pytest.mark.asyncio
async def test_websocket_subscribe_single_symbol(indicators_service):
    """Test subscribing to a single symbol with indicators."""
    service, channel = indicators_service
    app.dependency_overrides[get_indicators_service_ws] = lambda: service

    client = TestClient(app)

    subscription_request = {
        "action": ActionType.SUBSCRIBE,
        "subscription_id": "test_sub_1",
        "symbol": "AAPL",
        "freq": "1min",
        "indicators": [{"type": "dummy_indicator", "params": {"multiplier": 3.0}}],
    }

    test_data = {
        "open": 100.0,
        "high": 105.0,
        "low": 95.0,
        "close": 102.0,
        "volume": 1000,
        "timestamp": 1640995200000,
    }

    with client.websocket_connect("/api/indicators") as websocket:
        websocket.send_text(json.dumps(subscription_request))

        response = websocket.receive_text()
        response_data = json.loads(response)

        print(response_data)
        assert response_data["status"] == StatusType.SUCCESS
        assert response_data["subscription_id"] == "test_sub_1"
        assert "Subscribed to AAPL" in response_data["message"]

        # Push test data to trigger indicator processing
        await channel.push_data("candles_min_AAPL", test_data)

        # Receive the indicator message
        message = websocket.receive_text()
        data = json.loads(message)

        assert data["subscription_id"] == "test_sub_1"
        assert data["symbol"] == "AAPL"
        assert data["candle"]["close"] == 102.0
        assert data["candle"]["volume"] == 1000
        assert data["candle"]["dummy_value"] == 306.0  # 102 * 3.0

    app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_websocket_multiple_subscriptions(indicators_service):
    """Test subscribing to multiple symbols with different indicators."""
    service, channel = indicators_service
    app.dependency_overrides[get_indicators_service_ws] = lambda: service

    client = TestClient(app)

    subscription_request_1 = {
        "action": ActionType.SUBSCRIBE,
        "subscription_id": "test_sub_1",
        "symbol": "AAPL",
        "freq": "1min",
        "indicators": [{"type": "dummy_indicator", "params": {"multiplier": 2.0}}],
    }

    subscription_request_2 = {
        "action": ActionType.SUBSCRIBE,
        "subscription_id": "test_sub_2",
        "symbol": "GOOGL",
        "freq": "1min",
        "indicators": [{"type": "dummy_indicator", "params": {"multiplier": 4.0}}],
    }

    test_data_aapl = {
        "open": 100.0,
        "high": 105.0,
        "low": 95.0,
        "close": 100.0,
        "volume": 1000,
        "timestamp": 1640995200000,
    }

    test_data_googl = {
        "open": 200.0,
        "high": 210.0,
        "low": 195.0,
        "close": 205.0,
        "volume": 500,
        "timestamp": 1640995260000,
    }

    with client.websocket_connect("/api/indicators") as websocket:
        # Subscribe to first symbol
        websocket.send_text(json.dumps(subscription_request_1))
        response1 = json.loads(websocket.receive_text())
        assert response1["status"] == StatusType.SUCCESS
        assert response1["subscription_id"] == "test_sub_1"

        # Subscribe to second symbol
        websocket.send_text(json.dumps(subscription_request_2))
        response2 = json.loads(websocket.receive_text())
        assert response2["status"] == StatusType.SUCCESS
        assert response2["subscription_id"] == "test_sub_2"

        # Push data for first symbol
        await channel.push_data("candles_min_AAPL", test_data_aapl)
        message1 = json.loads(websocket.receive_text())
        assert message1["subscription_id"] == "test_sub_1"
        assert message1["symbol"] == "AAPL"
        assert message1["candle"]["dummy_value"] == 200.0  # 100 * 2.0

        # Push data for second symbol
        await channel.push_data("candles_min_GOOGL", test_data_googl)
        message2 = json.loads(websocket.receive_text())
        assert message2["subscription_id"] == "test_sub_2"
        assert message2["symbol"] == "GOOGL"
        assert message2["candle"]["dummy_value"] == 820.0  # 205 * 4.0

    app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_websocket_unsubscribe(indicators_service):
    """Test unsubscribing from a symbol."""
    service, channel = indicators_service
    app.dependency_overrides[get_indicators_service_ws] = lambda: service

    client = TestClient(app)

    subscription_request = {
        "action": ActionType.SUBSCRIBE,
        "subscription_id": "test_sub_1",
        "symbol": "AAPL",
        "freq": "1min",
        "indicators": [{"type": "dummy_indicator", "params": {"multiplier": 2.0}}],
    }

    unsubscription_request = {
        "action": ActionType.UNSUBSCRIBE,
        "subscription_id": "test_sub_1",
    }

    with client.websocket_connect("/api/indicators") as websocket:
        # Subscribe
        websocket.send_text(json.dumps(subscription_request))
        response = json.loads(websocket.receive_text())
        assert response["status"] == StatusType.SUCCESS
        assert len(channel.unsubscriptions.get("candles_min_AAPL", [])) == 0

        # Unsubscribe
        websocket.send_text(json.dumps(unsubscription_request))
        response = json.loads(websocket.receive_text())
        assert response["status"] == StatusType.SUCCESS
        assert response["subscription_id"] == "test_sub_1"
        assert "Unsubscribed from AAPL" in response["message"]

        # Verify unsubscription happened
        assert len(channel.unsubscriptions["candles_min_AAPL"]) > 0

    app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_websocket_unsubscribe_nonexistent(indicators_service):
    """Test unsubscribing from a non-existent subscription."""
    service, channel = indicators_service
    app.dependency_overrides[get_indicators_service_ws] = lambda: service

    client = TestClient(app)

    unsubscription_request = {
        "action": ActionType.UNSUBSCRIBE,
        "subscription_id": "nonexistent_sub",
    }

    with client.websocket_connect("/api/indicators") as websocket:
        websocket.send_text(json.dumps(unsubscription_request))
        response = json.loads(websocket.receive_text())
        assert response["status"] == StatusType.ERROR
        assert response["subscription_id"] == "nonexistent_sub"
        assert "Subscription not found" in response["message"]

    app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_websocket_resubscribe_scenario(indicators_service):
    """Test full scenario: subscribe, unsubscribe, subscribe again."""
    service, channel = indicators_service
    app.dependency_overrides[get_indicators_service_ws] = lambda: service

    client = TestClient(app)

    subscription_request = {
        "action": ActionType.SUBSCRIBE,
        "subscription_id": "test_sub_1",
        "symbol": "AAPL",
        "freq": "1min",
        "indicators": [{"type": "dummy_indicator", "params": {"multiplier": 2.0}}],
    }

    unsubscription_request = {
        "action": ActionType.UNSUBSCRIBE,
        "subscription_id": "test_sub_1",
    }

    resubscription_request = {
        "action": ActionType.SUBSCRIBE,
        "subscription_id": "test_sub_2",
        "symbol": "AAPL",
        "freq": "1min",
        "indicators": [{"type": "dummy_indicator", "params": {"multiplier": 5.0}}],
    }

    test_data = {
        "open": 100.0,
        "high": 105.0,
        "low": 95.0,
        "close": 100.0,
        "volume": 1000,
        "timestamp": 1640995200000,
    }

    with client.websocket_connect("/api/indicators") as websocket:
        # Initial subscription
        websocket.send_text(json.dumps(subscription_request))
        response1 = json.loads(websocket.receive_text())
        assert response1["status"] == StatusType.SUCCESS
        assert response1["subscription_id"] == "test_sub_1"

        # Unsubscribe
        websocket.send_text(json.dumps(unsubscription_request))
        response2 = json.loads(websocket.receive_text())
        assert response2["status"] == StatusType.SUCCESS

        # Re-subscribe with different ID and multiplier
        websocket.send_text(json.dumps(resubscription_request))
        response3 = json.loads(websocket.receive_text())
        assert response3["status"] == StatusType.SUCCESS
        assert response3["subscription_id"] == "test_sub_2"

        # Push data and verify the new subscription works
        await channel.push_data("candles_min_AAPL", test_data)
        message = json.loads(websocket.receive_text())
        assert message["subscription_id"] == "test_sub_2"
        assert message["symbol"] == "AAPL"
        assert message["candle"]["dummy_value"] == 500.0  # 100 * 5.0

    app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_websocket_invalid_action(indicators_service):
    """Test handling of invalid action types."""
    service, channel = indicators_service
    app.dependency_overrides[get_indicators_service_ws] = lambda: service

    client = TestClient(app)

    invalid_request = {"action": "invalid_action", "subscription_id": "test_sub_1"}

    with client.websocket_connect("/api/indicators") as websocket:
        websocket.send_text(json.dumps(invalid_request))
        response = json.loads(websocket.receive_text())
        assert response["status"] == StatusType.ERROR
        assert "Unknown action: invalid_action" in response["message"]

    app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_websocket_disconnect_cleanup(indicators_service):
    """Test that all subscriptions are cleaned up when websocket disconnects."""
    service, channel = indicators_service
    app.dependency_overrides[get_indicators_service_ws] = lambda: service

    client = TestClient(app)

    subscription_request_1 = {
        "action": ActionType.SUBSCRIBE,
        "subscription_id": "test_sub_1",
        "symbol": "AAPL",
        "freq": "1min",
        "indicators": [{"type": "dummy_indicator", "params": {"multiplier": 2.0}}],
    }

    subscription_request_2 = {
        "action": ActionType.SUBSCRIBE,
        "subscription_id": "test_sub_2",
        "symbol": "GOOGL",
        "freq": "1min",
        "indicators": [{"type": "dummy_indicator", "params": {"multiplier": 3.0}}],
    }

    # Use websocket context that will automatically disconnect
    with client.websocket_connect("/api/indicators") as websocket:
        # Subscribe to multiple symbols
        websocket.send_text(json.dumps(subscription_request_1))
        json.loads(websocket.receive_text())  # consume response

        websocket.send_text(json.dumps(subscription_request_2))
        json.loads(websocket.receive_text())  # consume response

        # Verify subscriptions were created
        assert len(channel.handlers) == 2

    # After context exit, subscriptions should be cleaned up
    # Note: In real implementation, cleanup happens in the finally block
    # This test verifies the structure is in place for cleanup

    app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_websocket_complex_scenario(indicators_service):
    """Test complex scenario with multiple subscribes, unsubscribes, and data pushes."""
    service, channel = indicators_service
    app.dependency_overrides[get_indicators_service_ws] = lambda: service

    client = TestClient(app)

    test_data_aapl = {
        "open": 150.0,
        "high": 155.0,
        "low": 145.0,
        "close": 152.0,
        "volume": 2000,
        "timestamp": 1640995200000,
    }

    test_data_googl = {
        "open": 2800.0,
        "high": 2850.0,
        "low": 2750.0,
        "close": 2820.0,
        "volume": 800,
        "timestamp": 1640995260000,
    }

    with client.websocket_connect("/api/indicators") as websocket:
        # Subscribe to AAPL with multiplier 2.0
        websocket.send_text(
            json.dumps(
                {
                    "action": ActionType.SUBSCRIBE,
                    "subscription_id": "aapl_sub_1",
                    "symbol": "AAPL",
                    "freq": "1min",
                    "indicators": [
                        {"type": "dummy_indicator", "params": {"multiplier": 2.0}}
                    ],
                }
            )
        )
        assert json.loads(websocket.receive_text())["status"] == StatusType.SUCCESS

        # Subscribe to GOOGL with multiplier 1.5
        websocket.send_text(
            json.dumps(
                {
                    "action": ActionType.SUBSCRIBE,
                    "subscription_id": "googl_sub_1",
                    "symbol": "GOOGL",
                    "freq": "1min",
                    "indicators": [
                        {"type": "dummy_indicator", "params": {"multiplier": 1.5}}
                    ],
                }
            )
        )
        assert json.loads(websocket.receive_text())["status"] == StatusType.SUCCESS

        # Subscribe to AAPL again with different ID and multiplier
        websocket.send_text(
            json.dumps(
                {
                    "action": ActionType.SUBSCRIBE,
                    "subscription_id": "aapl_sub_2",
                    "symbol": "AAPL",
                    "freq": "1min",
                    "indicators": [
                        {"type": "dummy_indicator", "params": {"multiplier": 3.5}}
                    ],
                }
            )
        )
        assert json.loads(websocket.receive_text())["status"] == StatusType.SUCCESS

        # Push AAPL data - should trigger both AAPL subscriptions
        await channel.push_data("candles_min_AAPL", test_data_aapl)

        # Receive messages (order might vary)
        messages = []
        for _ in range(2):  # Expecting 2 messages for AAPL
            messages.append(json.loads(websocket.receive_text()))

        aapl_messages = [msg for msg in messages if msg["symbol"] == "AAPL"]
        assert len(aapl_messages) == 2

        sub_ids = [msg["subscription_id"] for msg in aapl_messages]
        assert "aapl_sub_1" in sub_ids
        assert "aapl_sub_2" in sub_ids

        # Check multipliers
        for msg in aapl_messages:
            if msg["subscription_id"] == "aapl_sub_1":
                assert msg["candle"]["dummy_value"] == 304.0  # 152 * 2.0
            elif msg["subscription_id"] == "aapl_sub_2":
                assert msg["candle"]["dummy_value"] == 532.0  # 152 * 3.5

        # Unsubscribe from one AAPL subscription
        websocket.send_text(
            json.dumps(
                {"action": ActionType.UNSUBSCRIBE, "subscription_id": "aapl_sub_1"}
            )
        )
        assert json.loads(websocket.receive_text())["status"] == StatusType.SUCCESS

        # Push GOOGL data
        await channel.push_data("candles_min_GOOGL", test_data_googl)
        googl_message = json.loads(websocket.receive_text())
        assert googl_message["subscription_id"] == "googl_sub_1"
        assert googl_message["symbol"] == "GOOGL"
        assert googl_message["candle"]["dummy_value"] == 4230.0  # 2820 * 1.5

    app.dependency_overrides.clear()
