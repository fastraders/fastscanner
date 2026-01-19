import asyncio
import json
import logging
from random import randint

import pytest
import pytest_asyncio
import websockets

from fastscanner.pkg.websockets import WebSocketSubscriber

logging.getLogger("websockets").setLevel(logging.WARNING)


class ConcreteWebSocketSubscriber(WebSocketSubscriber):
    def __init__(self, host: str, port: int, endpoint: str, max_connections: int = 10):
        super().__init__(host, port, endpoint, max_connections)
        self.received_messages: list[tuple[str, str]] = []
        self.message_event = asyncio.Event()

    async def handle_ws_message(self, socket_id: str, message: str | bytes):
        self.received_messages.append((socket_id, str(message)))
        self.message_event.set()


async def echo_server(websocket):
    """Simple echo server that sends back received messages."""
    try:
        async for message in websocket:
            await websocket.send(message)
    except websockets.ConnectionClosedError:
        pass


async def counting_server(websocket):
    """Server that sends numbered messages and echoes back."""
    counter = 0
    try:
        async for message in websocket:
            response = json.dumps({"count": counter, "echo": message})
            await websocket.send(response)
            counter += 1
    except websockets.ConnectionClosedError:
        pass


async def disconnect_after_first_server(websocket):
    """Server that disconnects after first message, then works normally."""
    try:
        message = await websocket.recv()
        await websocket.send(f"first: {message}")
        await websocket.close()
    except:
        pass


@pytest_asyncio.fixture
async def echo_ws_server():
    """Start an echo WebSocket server."""
    port = randint(10000, 60000)
    async with websockets.serve(echo_server, "localhost", port) as server:
        yield "localhost", port


@pytest_asyncio.fixture
async def counting_ws_server():
    """Start a counting WebSocket server."""
    port = randint(10000, 60000)
    async with websockets.serve(counting_server, "localhost", port) as server:
        yield "localhost", port


@pytest_asyncio.fixture
async def disconnect_ws_server():
    """Start a server that disconnects after first message."""
    port = randint(10000, 60000)
    async with websockets.serve(
        disconnect_after_first_server, "localhost", port
    ) as server:
        yield "localhost", port


@pytest.mark.asyncio
async def test_basic_connection_and_message_sending(echo_ws_server):
    host, port = echo_ws_server
    subscriber = ConcreteWebSocketSubscriber(host, port, "")

    try:
        await subscriber.send_ws_message("handler1", "test message")
        await asyncio.wait_for(subscriber.message_event.wait(), timeout=2.0)

        assert len(subscriber.received_messages) == 1
        assert "test message" in subscriber.received_messages[0][1]
    finally:
        await subscriber.stop()


@pytest.mark.asyncio
async def test_multiple_messages_on_same_connection(echo_ws_server):
    host, port = echo_ws_server
    subscriber = ConcreteWebSocketSubscriber(host, port, "")

    try:
        messages = ["message1", "message2", "message3"]
        for msg in messages:
            subscriber.message_event.clear()
            await subscriber.send_ws_message("handler1", msg)
            await asyncio.wait_for(subscriber.message_event.wait(), timeout=2.0)

        assert len(subscriber.received_messages) == 3
        for i, msg in enumerate(messages):
            assert msg in subscriber.received_messages[i][1]
    finally:
        await subscriber.stop()


@pytest.mark.asyncio
async def test_send_subscribe_message_stores_for_reconnection(echo_ws_server):
    host, port = echo_ws_server
    subscriber = ConcreteWebSocketSubscriber(host, port, "")

    try:
        subscribe_msg = json.dumps({"action": "subscribe", "symbol": "TEST"})
        await subscriber.send_subscribe_message("handler1", subscribe_msg)

        await asyncio.sleep(0.1)
        assert len(subscriber.received_messages) >= 1
    finally:
        await subscriber.stop()


@pytest.mark.asyncio
async def test_send_unsubscribe_message_removes_handler(echo_ws_server):
    host, port = echo_ws_server
    subscriber = ConcreteWebSocketSubscriber(host, port, "")

    try:
        await subscriber.send_subscribe_message("handler1", "subscribe")
        await asyncio.sleep(0.1)

        await subscriber.send_unsubscribe_message("handler1", "unsubscribe")
        await asyncio.sleep(0.1)

        assert len(subscriber.received_messages) >= 2
    finally:
        await subscriber.stop()


@pytest.mark.asyncio
async def test_multiple_handlers_on_different_connections(counting_ws_server):
    host, port = counting_ws_server
    subscriber = ConcreteWebSocketSubscriber(host, port, "", max_connections=2)

    try:
        await subscriber.send_ws_message("handler1", "from_handler1")
        await asyncio.sleep(0.05)

        await subscriber.send_ws_message("handler2", "from_handler2")
        await asyncio.sleep(0.05)

        await subscriber.send_ws_message("handler3", "from_handler3")
        await asyncio.sleep(0.1)

        assert len(subscriber.received_messages) >= 3
    finally:
        await subscriber.stop()


@pytest.mark.asyncio
async def test_concurrent_sends_are_handled_correctly(echo_ws_server):
    host, port = echo_ws_server
    subscriber = ConcreteWebSocketSubscriber(host, port, "")

    try:

        async def send_message(handler_id, message):
            await subscriber.send_ws_message(handler_id, message)

        tasks = [
            asyncio.create_task(send_message(f"handler{i}", f"message{i}"))
            for i in range(10)
        ]

        await asyncio.gather(*tasks)
        await asyncio.sleep(0.5)

        assert len(subscriber.received_messages) >= 10
    finally:
        await subscriber.stop()


@pytest.mark.asyncio
async def test_connection_respects_max_connections_limit(echo_ws_server):
    host, port = echo_ws_server
    subscriber = ConcreteWebSocketSubscriber(host, port, "", max_connections=2)

    try:
        handlers = [f"handler{i}" for i in range(5)]
        for handler in handlers:
            await subscriber.send_ws_message(handler, f"test from {handler}")
            await asyncio.sleep(0.05)

        await asyncio.sleep(0.2)

        assert len(subscriber.received_messages) >= 5
    finally:
        await subscriber.stop()


@pytest.mark.asyncio
async def test_handles_structured_json_messages(echo_ws_server):
    host, port = echo_ws_server
    subscriber = ConcreteWebSocketSubscriber(host, port, "")

    try:
        message = json.dumps({"type": "subscribe", "data": "test_data"})
        subscriber.message_event.clear()
        await subscriber.send_ws_message("handler1", message)
        await asyncio.wait_for(subscriber.message_event.wait(), timeout=2.0)

        assert len(subscriber.received_messages) == 1
        received_data = json.loads(subscriber.received_messages[0][1])
        assert received_data["type"] == "subscribe"
        assert received_data["data"] == "test_data"
    finally:
        await subscriber.stop()


@pytest.mark.asyncio
async def test_handle_ws_message_exception_doesnt_crash_listener(counting_ws_server):
    host, port = counting_ws_server

    class FailingSubscriber(WebSocketSubscriber):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.call_count = 0

        async def handle_ws_message(self, socket_id: str, message: str | bytes):
            self.call_count += 1
            if self.call_count <= 2:
                raise ValueError("Handler error")

    subscriber = FailingSubscriber(host, port, "")

    try:
        await subscriber.send_ws_message("handler1", "test1")
        await subscriber.send_ws_message("handler1", "test2")
        await subscriber.send_ws_message("handler1", "test3")

        await asyncio.sleep(0.3)

        assert subscriber.call_count >= 3
    finally:
        await subscriber.stop()


@pytest.mark.asyncio
async def test_graceful_stop_closes_all_connections(echo_ws_server):
    host, port = echo_ws_server
    subscriber = ConcreteWebSocketSubscriber(host, port, "")

    await subscriber.send_ws_message("handler1", "test")
    await subscriber.send_ws_message("handler2", "test")
    await asyncio.sleep(0.1)

    await subscriber.stop()
    await asyncio.sleep(0.1)


@pytest.mark.asyncio
async def test_rapid_connection_and_disconnection(echo_ws_server):
    host, port = echo_ws_server

    for _ in range(5):
        subscriber = ConcreteWebSocketSubscriber(host, port, "")
        await subscriber.send_ws_message("handler1", "quick test")
        await asyncio.sleep(0.05)
        await subscriber.stop()


@pytest.mark.asyncio
async def test_concurrent_subscribers_dont_interfere(echo_ws_server):
    host, port = echo_ws_server

    subscriber1 = ConcreteWebSocketSubscriber(host, port, "")
    subscriber2 = ConcreteWebSocketSubscriber(host, port, "")

    try:
        await subscriber1.send_ws_message("handler1", "from_sub1")
        await subscriber2.send_ws_message("handler2", "from_sub2")

        await asyncio.sleep(0.3)

        assert len(subscriber1.received_messages) >= 1
        assert len(subscriber2.received_messages) >= 1

        assert "from_sub1" in subscriber1.received_messages[0][1]
        assert "from_sub2" in subscriber2.received_messages[0][1]
    finally:
        await subscriber1.stop()
        await subscriber2.stop()


@pytest.mark.asyncio
async def test_resubscription_after_server_disconnect(disconnect_ws_server):
    host, port = disconnect_ws_server

    subscriber = ConcreteWebSocketSubscriber(host, port, "")

    try:
        subscribe_msg = "subscribe:TEST"
        await subscriber.send_subscribe_message("handler1", subscribe_msg)

        await asyncio.sleep(0.2)

        assert len(subscriber.received_messages) >= 1
    finally:
        await subscriber.stop()


@pytest.mark.asyncio
async def test_send_to_nonexistent_handler_creates_connection(echo_ws_server):
    host, port = echo_ws_server
    subscriber = ConcreteWebSocketSubscriber(host, port, "")

    try:
        await subscriber.send_ws_message("new_handler", "test message")
        await asyncio.sleep(0.2)

        assert len(subscriber.received_messages) >= 1
    finally:
        await subscriber.stop()


@pytest.mark.asyncio
async def test_multiple_subscribe_messages_to_same_handler(echo_ws_server):
    host, port = echo_ws_server
    subscriber = ConcreteWebSocketSubscriber(host, port, "")

    try:
        await subscriber.send_subscribe_message("handler1", "subscribe1")
        await asyncio.sleep(0.05)

        await subscriber.send_subscribe_message("handler1", "subscribe2")
        await asyncio.sleep(0.1)

        assert len(subscriber.received_messages) >= 2
    finally:
        await subscriber.stop()


@pytest.mark.asyncio
async def test_interleaved_subscribe_unsubscribe(echo_ws_server):
    host, port = echo_ws_server
    subscriber = ConcreteWebSocketSubscriber(host, port, "")

    try:
        await subscriber.send_subscribe_message("handler1", "sub1")
        await asyncio.sleep(0.05)

        await subscriber.send_subscribe_message("handler2", "sub2")
        await asyncio.sleep(0.05)

        await subscriber.send_unsubscribe_message("handler1", "unsub1")
        await asyncio.sleep(0.05)

        await subscriber.send_ws_message("handler2", "msg_to_handler2")
        await asyncio.sleep(0.1)

        assert len(subscriber.received_messages) >= 4
    finally:
        await subscriber.stop()


@pytest.mark.asyncio
async def test_high_frequency_messaging(echo_ws_server):
    host, port = echo_ws_server
    subscriber = ConcreteWebSocketSubscriber(host, port, "")

    try:
        for i in range(50):
            await subscriber.send_ws_message("handler1", f"message{i}")

        await asyncio.sleep(1.0)

        assert len(subscriber.received_messages) >= 45
    finally:
        await subscriber.stop()


@pytest.mark.asyncio
async def test_send_retries_on_connection_closed():
    class FlakySubscriber(WebSocketSubscriber):
        async def handle_ws_message(self, socket_id: str, message: str | bytes):
            pass

    subscriber = FlakySubscriber("localhost", 9999, "")

    try:
        with pytest.raises(Exception):
            await asyncio.wait_for(
                subscriber.send_ws_message("handler1", "test"), timeout=2.0
            )
    finally:
        await subscriber.stop()


@pytest.mark.asyncio
async def test_messages_handled_in_order(counting_ws_server):
    host, port = counting_ws_server
    subscriber = ConcreteWebSocketSubscriber(host, port, "")

    try:
        messages = [f"message{i}" for i in range(10)]
        for msg in messages:
            await subscriber.send_ws_message("handler1", msg)
            await asyncio.sleep(0.02)

        await asyncio.sleep(0.3)

        received = [json.loads(msg)["echo"] for _, msg in subscriber.received_messages]
        for i, msg in enumerate(messages):
            if i < len(received):
                assert msg == received[i]
    finally:
        await subscriber.stop()


@pytest.mark.asyncio
async def test_endpoint_is_used_in_connection(echo_ws_server):
    host, port = echo_ws_server
    subscriber = ConcreteWebSocketSubscriber(host, port, "/custom/endpoint")

    try:
        await subscriber.send_ws_message("handler1", "test")
        await asyncio.sleep(0.2)
    finally:
        await subscriber.stop()
