"""Tests for indicator pydantic models."""

import pytest
from fastscanner.client.indicators import (
    CumulativeDailyVolume,
    PremarketCumulative,
    ATR,
    PositionInRange,
    DailyATR,
    MarketCap,
    CumulativeOperation
)
from fastscanner.services.indicators.service import IndicatorParams


def test_cumulative_daily_volume():
    """Test CumulativeDailyVolume model."""
    indicator = CumulativeDailyVolume()
    params = indicator.to_params()
    
    assert isinstance(params, IndicatorParams)
    assert params.type_ == "cumulative_daily_volume"
    assert params.params == {}


def test_premarket_cumulative():
    """Test PremarketCumulative model."""
    indicator = PremarketCumulative(candle_col="volume", op=CumulativeOperation.MAX)
    params = indicator.to_params()
    
    assert isinstance(params, IndicatorParams)
    assert params.type_ == "premarket_cumulative"
    assert params.params == {"candle_col": "volume", "op": "max"}


def test_atr():
    """Test ATR model."""
    indicator = ATR(period=14, freq="1min")
    params = indicator.to_params()
    
    assert isinstance(params, IndicatorParams)
    assert params.type_ == "atr"
    assert params.params == {"period": 14, "freq": "1min"}


def test_position_in_range():
    """Test PositionInRange model."""
    indicator = PositionInRange(n_days=5)
    params = indicator.to_params()
    
    assert isinstance(params, IndicatorParams)
    assert params.type_ == "position_in_range"
    assert params.params == {"n_days": 5}


def test_daily_atr():
    """Test DailyATR model."""
    indicator = DailyATR(period=20)
    params = indicator.to_params()
    
    assert isinstance(params, IndicatorParams)
    assert params.type_ == "daily_atr"
    assert params.params == {"period": 20}


def test_market_cap():
    """Test MarketCap model."""
    indicator = MarketCap()
    params = indicator.to_params()
    
    assert isinstance(params, IndicatorParams)
    assert params.type_ == "market_cap"
    assert params.params == {}


def test_cumulative_operation_enum():
    """Test CumulativeOperation enum values."""
    assert CumulativeOperation.MIN.value == "min"
    assert CumulativeOperation.MAX.value == "max" 
    assert CumulativeOperation.SUM.value == "sum"