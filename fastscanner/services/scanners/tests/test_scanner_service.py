import pytest

from fastscanner.services.scanners.ports import ScannerParams


@pytest.mark.asyncio
async def test_realtime_data_flow_scanner_passes(
    scanner_service, scanner_params, subscription_handler
):
    service, channel = scanner_service

    scanner_id = await service.subscribe_realtime(
        params=scanner_params, handler=subscription_handler, freq="1min"
    )

    test_data = {
        "open": 100.0,
        "high": 105.0,
        "low": 95.0,
        "close": 60.0,  # Above min_value of 50.0
        "volume": 1000,
        "timestamp": 1640995200000,  # 2022-01-01 00:00:00 UTC
    }

    await channel.push_data("candles_min_AAPL", test_data)

    assert len(subscription_handler.handled_symbols) == 1
    assert subscription_handler.handled_symbols[0] == "AAPL"
    assert bool(subscription_handler.handled_passed[0]) is True

    handled_row = subscription_handler.handled_rows[0]
    assert "test_indicator" in handled_row
    assert handled_row["test_indicator"] == 120.0  # close * 2


@pytest.mark.asyncio
async def test_realtime_data_flow_scanner_fails(
    scanner_service, scanner_params, subscription_handler
):
    service, channel = scanner_service

    scanner_id = await service.subscribe_realtime(
        params=scanner_params, handler=subscription_handler, freq="1min"
    )

    test_data = {
        "open": 40.0,
        "high": 45.0,
        "low": 35.0,
        "close": 30.0,  # Below min_value of 50.0
        "volume": 1000,
        "timestamp": 1640995200000,
    }

    await channel.push_data("candles_min_AAPL", test_data)

    assert len(subscription_handler.handled_symbols) == 1
    assert subscription_handler.handled_symbols[0] == "AAPL"
    assert bool(subscription_handler.handled_passed[0]) is False

    handled_row = subscription_handler.handled_rows[0]
    assert "test_indicator" in handled_row
    assert handled_row["test_indicator"] == 60.0  # close * 2


@pytest.mark.asyncio
async def test_realtime_data_flow_multiple_symbols(
    scanner_service, scanner_params, subscription_handler
):
    service, channel = scanner_service

    scanner_id = await service.subscribe_realtime(
        params=scanner_params, handler=subscription_handler, freq="1min"
    )

    test_data_aapl = {
        "close": 60.0,  # Should pass
        "volume": 1000,
        "timestamp": 1640995200000,
    }

    test_data_googl = {
        "close": 40.0,  # Should fail
        "volume": 2000,
        "timestamp": 1640995200000,
    }

    await channel.push_data("candles_min_AAPL", test_data_aapl)
    await channel.push_data("candles_min_GOOGL", test_data_googl)

    assert len(subscription_handler.handled_symbols) == 2
    assert "AAPL" in subscription_handler.handled_symbols
    assert "GOOGL" in subscription_handler.handled_symbols

    aapl_idx = subscription_handler.handled_symbols.index("AAPL")
    googl_idx = subscription_handler.handled_symbols.index("GOOGL")

    assert bool(subscription_handler.handled_passed[aapl_idx]) is True
    assert bool(subscription_handler.handled_passed[googl_idx]) is False


@pytest.mark.asyncio
async def test_realtime_data_flow_different_scanner_params(
    scanner_service, subscription_handler
):
    service, channel = scanner_service

    params = ScannerParams(type_="dummy_scanner", params={"min_value": 100.0})
    scanner_id = await service.subscribe_realtime(
        params=params, handler=subscription_handler, freq="1min"
    )

    test_data = {
        "close": 75.0,  # Between 50 and 100
        "volume": 1000,
        "timestamp": 1640995200000,
    }

    await channel.push_data("candles_min_AAPL", test_data)

    assert bool(subscription_handler.handled_passed[0]) is False


@pytest.mark.asyncio
async def test_scanner_service_subscription_behavior(
    scanner_service, scanner_params, subscription_handler
):
    service, channel = scanner_service

    scanner_id = await service.subscribe_realtime(
        scanner_params, subscription_handler, "1min"
    )
    assert scanner_id is not None

    await service.unsubscribe_realtime(scanner_id)

    await service.unsubscribe_realtime("non-existent")


@pytest.mark.asyncio
async def test_data_type_conversion(
    scanner_service, scanner_params, subscription_handler
):
    service, channel = scanner_service

    scanner_id = await service.subscribe_realtime(
        scanner_params, subscription_handler, "1min"
    )

    test_data = {
        "open": "100.5",
        "high": "105.5",
        "low": "95.5",
        "close": "60.5",
        "volume": "1000",
        "timestamp": 1640995200000,
    }

    await channel.push_data("candles_min_AAPL", test_data)

    handled_row = subscription_handler.handled_rows[0]
    assert isinstance(handled_row["open"], float)
    assert isinstance(handled_row["high"], float)
    assert isinstance(handled_row["low"], float)
    assert isinstance(handled_row["close"], float)
    assert isinstance(handled_row["volume"], float)
