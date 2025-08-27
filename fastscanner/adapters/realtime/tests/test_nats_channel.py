import json
from unittest.mock import AsyncMock, Mock

import pytest

from fastscanner.adapters.realtime.nats_channel import NATSChannel


class MockHandler:
    def __init__(self, handler_id: str):
        self._id = handler_id
        self.handle = AsyncMock()

    def id(self) -> str:
        return self._id


@pytest.fixture
def nats_channel():
    return NATSChannel(
        servers=["nats://localhost:4222"],
    )


@pytest.fixture
def mock_nats_connect(monkeypatch):
    mock_nc = AsyncMock()
    mock_nc.is_closed = False
    mock_connect = AsyncMock(return_value=mock_nc)
    monkeypatch.setattr("nats.connect", mock_connect)
    return mock_connect, mock_nc


def test_init(nats_channel):
    assert nats_channel._servers == ["nats://localhost:4222"]
    assert nats_channel._nc is None
    assert nats_channel._handlers == {}
    assert nats_channel._subscriptions == {}
    assert nats_channel._pending_messages == []
    assert not nats_channel._is_stopped


def test_nc_property_raises_when_not_connected(nats_channel):
    with pytest.raises(RuntimeError, match="NATS connection not established"):
        _ = nats_channel.nc


@pytest.mark.asyncio
async def test_ensure_connection(nats_channel, mock_nats_connect):
    mock_connect, mock_nc = mock_nats_connect

    await nats_channel._ensure_connection()

    mock_connect.assert_called_once_with(servers=["nats://localhost:4222"])
    assert nats_channel._nc == mock_nc


@pytest.mark.asyncio
async def test_nc_property_returns_connection(nats_channel, mock_nats_connect):
    mock_connect, mock_nc = mock_nats_connect

    await nats_channel._ensure_connection()
    assert nats_channel.nc == mock_nc


@pytest.mark.asyncio
async def test_push_with_flush(nats_channel, mock_nats_connect):
    mock_connect, mock_nc = mock_nats_connect

    test_data = {"key": "value"}
    await nats_channel.push("test_channel", test_data, flush=True)

    mock_nc.publish.assert_called_once_with(
        "test_channel", json.dumps(test_data).encode()
    )


@pytest.mark.asyncio
async def test_push_without_flush(nats_channel):
    test_data = {"key": "value"}
    await nats_channel.push("test_channel", test_data, flush=False)

    assert nats_channel._pending_messages == [("test_channel", test_data)]


@pytest.mark.asyncio
async def test_flush(nats_channel, mock_nats_connect):
    mock_connect, mock_nc = mock_nats_connect

    test_data1 = {"key1": "value1"}
    test_data2 = {"key2": "value2"}
    nats_channel._pending_messages = [
        ("channel1", test_data1),
        ("channel2", test_data2),
    ]

    await nats_channel.flush()

    assert mock_nc.publish.call_count == 2
    mock_nc.publish.assert_any_call("channel1", json.dumps(test_data1).encode())
    mock_nc.publish.assert_any_call("channel2", json.dumps(test_data2).encode())
    assert nats_channel._pending_messages == []


@pytest.mark.asyncio
async def test_subscribe(nats_channel, mock_nats_connect):
    mock_connect, mock_nc = mock_nats_connect
    mock_subscription = Mock()
    mock_nc.subscribe.return_value = mock_subscription

    handler = MockHandler("handler1")
    await nats_channel.subscribe("test_channel", handler)

    assert "test_channel" in nats_channel._handlers
    assert handler in nats_channel._handlers["test_channel"]
    assert nats_channel._subscriptions["test_channel"] == mock_subscription
    mock_nc.subscribe.assert_called_once()


@pytest.mark.asyncio
async def test_subscribe_duplicate_handler(nats_channel, mock_nats_connect):
    mock_connect, mock_nc = mock_nats_connect
    mock_subscription = Mock()
    mock_nc.subscribe.return_value = mock_subscription

    handler = MockHandler("handler1")
    await nats_channel.subscribe("test_channel", handler)
    await nats_channel.subscribe("test_channel", handler)

    assert len(nats_channel._handlers["test_channel"]) == 1


@pytest.mark.asyncio
async def test_message_handler(nats_channel, mock_nats_connect):
    mock_connect, mock_nc = mock_nats_connect

    handler1 = MockHandler("handler1")
    handler2 = MockHandler("handler2")
    nats_channel._handlers["test_channel"] = [handler1, handler2]

    mock_msg = Mock()
    test_data = {"test": "data"}
    mock_msg.data.decode.return_value = json.dumps(test_data)

    message_handler = nats_channel._message_handler("test_channel")
    await message_handler(mock_msg)

    handler1.handle.assert_called_once_with("test_channel", test_data)
    handler2.handle.assert_called_once_with("test_channel", test_data)


@pytest.mark.asyncio
async def test_unsubscribe_removes_handler(nats_channel):
    handler1 = MockHandler("handler1")
    handler2 = MockHandler("handler2")
    mock_subscription = AsyncMock()

    nats_channel._handlers["test_channel"] = [handler1, handler2]
    nats_channel._subscriptions["test_channel"] = mock_subscription

    await nats_channel.unsubscribe("test_channel", "handler1")

    assert len(nats_channel._handlers["test_channel"]) == 1
    assert handler2 in nats_channel._handlers["test_channel"]
    assert handler1 not in nats_channel._handlers["test_channel"]


@pytest.mark.asyncio
async def test_unsubscribe_removes_subscription_when_no_handlers(nats_channel):
    handler1 = MockHandler("handler1")
    mock_subscription = AsyncMock()

    nats_channel._handlers["test_channel"] = [handler1]
    nats_channel._subscriptions["test_channel"] = mock_subscription

    await nats_channel.unsubscribe("test_channel", "handler1")

    assert "test_channel" not in nats_channel._handlers
    assert "test_channel" not in nats_channel._subscriptions
    mock_subscription.unsubscribe.assert_called_once()


@pytest.mark.asyncio
async def test_reset(nats_channel):
    handler1 = MockHandler("handler1")
    mock_subscription = AsyncMock()
    mock_nc = Mock()
    mock_nc.is_closed = False
    mock_nc.close = AsyncMock()

    nats_channel._handlers["test_channel"] = [handler1]
    nats_channel._subscriptions["test_channel"] = mock_subscription
    nats_channel._pending_messages = [("channel", {"data": "test"})]
    nats_channel._nc = mock_nc

    await nats_channel.reset()

    assert nats_channel._is_stopped is True
    mock_subscription.unsubscribe.assert_called_once()
    assert len(nats_channel._subscriptions) == 0
    mock_nc.close.assert_called_once()
    assert len(nats_channel._handlers) == 0
    assert len(nats_channel._pending_messages) == 0
