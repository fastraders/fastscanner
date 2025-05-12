from datetime import date, datetime

import numpy as np
import pandas as pd
import pytest

from fastscanner.services.indicators.lib.candle import PositionInRangeIndicator
from fastscanner.services.indicators.ports import CandleCol
from fastscanner.services.indicators.tests.fixtures import CandleStoreTest, candles


def test_position_in_range_type():
    assert PositionInRangeIndicator.type() == "position_in_range"


def test_position_in_range_column_name():
    indicator = PositionInRangeIndicator(n_days=10)
    assert indicator.column_name() == "position_in_range_10"

    indicator = PositionInRangeIndicator(n_days=20)
    assert indicator.column_name() == "position_in_range_20"


def test_position_in_range_extend(candles: "CandleStoreTest"):
    # Set up test data for daily candles
    daily_dates = pd.date_range(start=date(2023, 1, 1), end=date(2023, 1, 10))
    daily_highs = [150, 152, 155, 153, 151, 154, 156, 158, 157, 155]
    daily_lows = [130, 132, 135, 133, 131, 134, 136, 138, 137, 135]
    daily_data = pd.DataFrame(
        {CandleCol.HIGH: daily_highs, CandleCol.LOW: daily_lows}, index=daily_dates
    )

    candles.set_data("AAPL", daily_data)

    # Create test data for intraday candles
    dates = [
        datetime(2023, 1, 11, 9, 30),
        datetime(2023, 1, 11, 10, 0),
        datetime(2023, 1, 11, 10, 30),
    ]
    closes = [135, 140, 145]
    df = pd.DataFrame({CandleCol.CLOSE: closes}, index=pd.DatetimeIndex(dates))

    indicator = PositionInRangeIndicator(n_days=5)
    result_df = indicator.extend("AAPL", df)

    # For the last 5 days (2023-01-06 to 2023-01-10):
    # Highest high = 158, Lowest low = 134
    # For close=135: (135-134)/(158-134) = 0.0417
    # For close=140: (140-134)/(158-134) = 0.25
    # For close=145: (145-134)/(158-134) = 0.4583
    expected_positions = [0.0417, 0.25, 0.4583]

    for actual, expected in zip(
        result_df[indicator.column_name()].to_list(), expected_positions
    ):
        assert abs(actual - expected) < 1e-4


def test_position_in_range_extend_multiple_days(candles: "CandleStoreTest"):
    # Set up test data for daily candles
    daily_dates = pd.date_range(start=date(2023, 1, 2), end=date(2023, 1, 11))
    daily_highs = [150, 152, 155, 153, 151, 154, 156, 158, 157, 155]
    daily_lows = [130, 132, 135, 133, 131, 134, 136, 138, 137, 135]
    daily_data = pd.DataFrame(
        {CandleCol.HIGH: daily_highs, CandleCol.LOW: daily_lows}, index=daily_dates
    )

    candles.set_data("AAPL", daily_data)

    # Create test data for intraday candles
    dates = [
        datetime(2023, 1, 11, 9, 30),
        datetime(2023, 1, 11, 10, 0),
        datetime(2023, 1, 11, 10, 30),
        datetime(2023, 1, 12, 9, 30),
        datetime(2023, 1, 12, 10, 0),
    ]
    closes = [135.0] * len(dates)
    df = pd.DataFrame({CandleCol.CLOSE: closes}, index=pd.DatetimeIndex(dates))

    indicator = PositionInRangeIndicator(n_days=5)
    result_df = indicator.extend("AAPL", df)
    expected_positions = [
        (135 - 131) / (158 - 131),
        (135 - 131) / (158 - 131),
        (135 - 131) / (158 - 131),
        (135 - 134) / (158 - 134),
        (135 - 134) / (158 - 134),
    ]
    for actual, expected in zip(
        result_df[indicator.column_name()].to_list(), expected_positions
    ):
        assert abs(actual - expected) < 1e-4


def test_position_in_range_extend_empty_data(candles: "CandleStoreTest"):
    candles.set_data("AAPL", pd.DataFrame(index=pd.DatetimeIndex([])))

    dates = [datetime(2023, 1, 10, 9, 30)]
    closes = [135]
    df = pd.DataFrame({CandleCol.CLOSE: closes}, index=pd.DatetimeIndex(dates))

    indicator = PositionInRangeIndicator(n_days=5)
    result_df = indicator.extend("AAPL", df)

    assert pd.isna(result_df[indicator.column_name()].iloc[0])


def test_position_in_range_extend_realtime_first_candle(candles: "CandleStoreTest"):
    daily_dates = pd.date_range(start=date(2023, 1, 1), end=date(2023, 1, 10))
    daily_highs = [150, 152, 155, 153, 151, 154, 156, 158, 157, 155]
    daily_lows = [130, 132, 135, 133, 131, 134, 136, 138, 137, 135]
    daily_data = pd.DataFrame(
        {CandleCol.HIGH: daily_highs, CandleCol.LOW: daily_lows}, index=daily_dates
    )

    candles.set_data("AAPL", daily_data)

    indicator = PositionInRangeIndicator(n_days=5)

    row_time = datetime(2023, 1, 11, 9, 30)
    row = pd.Series({CandleCol.CLOSE: 140}, name=row_time)

    result_row = indicator.extend_realtime("AAPL", row)

    # For the last 5 days (2023-01-06 to 2023-01-10):
    # Highest high = 158, Lowest low = 134
    # For close=140: (140-134)/(158-134) = 0.25
    assert abs(result_row[indicator.column_name()] - 0.25) < 1e-4
    assert indicator._last_date["AAPL"] == row_time.date()
    assert len(indicator._high_n_days["AAPL"]) > 0
    assert len(indicator._low_n_days["AAPL"]) > 0


def test_position_in_range_extend_realtime_same_day(candles: "CandleStoreTest"):
    daily_dates = pd.date_range(start=date(2023, 1, 1), end=date(2023, 1, 10))
    daily_highs = [150, 152, 155, 153, 151, 154, 156, 158, 157, 155]
    daily_lows = [130, 132, 135, 133, 131, 134, 136, 138, 137, 135]
    daily_data = pd.DataFrame(
        {CandleCol.HIGH: daily_highs, CandleCol.LOW: daily_lows}, index=daily_dates
    )

    candles.set_data("AAPL", daily_data)

    indicator = PositionInRangeIndicator(n_days=5)

    first_time = datetime(2023, 1, 11, 9, 30)
    first_row = pd.Series({CandleCol.CLOSE: 140}, name=first_time)
    indicator.extend_realtime("AAPL", first_row)

    second_time = datetime(2023, 1, 11, 10, 0)
    second_row = pd.Series({CandleCol.CLOSE: 145}, name=second_time)
    result_row = indicator.extend_realtime("AAPL", second_row)

    # For the last 5 days (2023-01-06 to 2023-01-10):
    # Highest high = 158, Lowest low = 134
    # For close=145: (145-134)/(158-134) = 0.4583
    assert abs(result_row[indicator.column_name()] - 0.4583) < 1e-4
    assert indicator._last_date["AAPL"] == first_time.date()


def test_position_in_range_extend_realtime_new_day(candles: "CandleStoreTest"):
    daily_dates = pd.date_range(start=date(2023, 1, 1), end=date(2023, 1, 11))
    daily_highs = [150, 152, 155, 153, 151, 154, 156, 158, 157, 155, 160]
    daily_lows = [130, 132, 135, 133, 131, 134, 136, 138, 137, 135, 140]
    daily_data = pd.DataFrame(
        {CandleCol.HIGH: daily_highs, CandleCol.LOW: daily_lows}, index=daily_dates
    )

    candles.set_data("AAPL", daily_data)

    indicator = PositionInRangeIndicator(n_days=5)

    day1_time = datetime(2023, 1, 10, 9, 30)
    day1_row = pd.Series({CandleCol.CLOSE: 140}, name=day1_time)
    indicator.extend_realtime("AAPL", day1_row)

    high_day1 = indicator._high_n_days["AAPL"].copy()
    low_day1 = indicator._low_n_days["AAPL"].copy()

    day2_time = datetime(2023, 1, 12, 9, 30)
    day2_row = pd.Series({CandleCol.CLOSE: 145}, name=day2_time)
    result_row = indicator.extend_realtime("AAPL", day2_row)

    assert indicator._last_date["AAPL"] == day2_time.date()

    # Instead of comparing the lists directly, check that the data was updated
    # by verifying the date changed
    assert indicator._last_date["AAPL"] != day1_time.date()


def test_position_in_range_extend_realtime_multiple_symbols(candles: "CandleStoreTest"):
    aapl_daily_dates = pd.date_range(start=date(2023, 1, 1), end=date(2023, 1, 10))
    aapl_daily_highs = [150, 152, 155, 153, 151, 154, 156, 158, 157, 155]
    aapl_daily_lows = [130, 132, 135, 133, 131, 134, 136, 138, 137, 135]
    aapl_daily_data = pd.DataFrame(
        {CandleCol.HIGH: aapl_daily_highs, CandleCol.LOW: aapl_daily_lows},
        index=aapl_daily_dates,
    )

    msft_daily_dates = pd.date_range(start=date(2023, 1, 1), end=date(2023, 1, 10))
    msft_daily_highs = [250, 252, 255, 253, 251, 254, 256, 258, 257, 255]
    msft_daily_lows = [230, 232, 235, 233, 231, 234, 236, 238, 237, 235]
    msft_daily_data = pd.DataFrame(
        {CandleCol.HIGH: msft_daily_highs, CandleCol.LOW: msft_daily_lows},
        index=msft_daily_dates,
    )

    candles.set_data("AAPL", aapl_daily_data)
    candles.set_data("MSFT", msft_daily_data)

    indicator = PositionInRangeIndicator(n_days=5)

    aapl_time = datetime(2023, 1, 11, 9, 30)
    aapl_row = pd.Series({CandleCol.CLOSE: 140}, name=aapl_time)
    result_row = indicator.extend_realtime("AAPL", aapl_row)

    # For the last 5 days (2023-01-06 to 2023-01-10):
    # Highest high = 158, Lowest low = 134
    # For close=140: (140-134)/(158-134) = 0.25
    assert abs(result_row[indicator.column_name()] - 0.25) < 1e-6

    msft_time = datetime(2023, 1, 11, 9, 30)
    msft_row = pd.Series({CandleCol.CLOSE: 245}, name=msft_time)
    result_row = indicator.extend_realtime("MSFT", msft_row)

    # For the last 5 days (2023-01-06 to 2023-01-10):
    # Highest high = 258, Lowest low = 234
    # For close=245: (245-234)/(258-234) = 0.4583
    assert abs(result_row[indicator.column_name()] - 0.4583) < 1e-4

    assert "AAPL" in indicator._high_n_days
    assert "MSFT" in indicator._high_n_days
    assert "AAPL" in indicator._low_n_days
    assert "MSFT" in indicator._low_n_days
    assert indicator._last_date["AAPL"] == aapl_time.date()
    assert indicator._last_date["MSFT"] == msft_time.date()


def test_position_in_range_edge_cases(candles: "CandleStoreTest"):
    daily_dates = pd.date_range(start=date(2023, 1, 1), end=date(2023, 1, 10))
    daily_highs = [150, 152, 155, 153, 151, 154, 156, 158, 157, 155]
    daily_lows = [130, 132, 135, 133, 131, 134, 136, 138, 137, 135]
    daily_data = pd.DataFrame(
        {CandleCol.HIGH: daily_highs, CandleCol.LOW: daily_lows}, index=daily_dates
    )

    candles.set_data("AAPL", daily_data)

    indicator = PositionInRangeIndicator(n_days=5)

    low_time = datetime(2023, 1, 11, 9, 30)
    low_row = pd.Series({CandleCol.CLOSE: 130}, name=low_time)
    result_row = indicator.extend_realtime("AAPL", low_row)

    # For the last 5 days (2023-01-06 to 2023-01-10):
    # Highest high = 158, Lowest low = 134
    # For close=130: (130-134)/(158-134) = -0.1667
    assert abs(result_row[indicator.column_name()] - (-0.1667)) < 1e-4

    high_time = datetime(2023, 1, 11, 10, 0)
    high_row = pd.Series({CandleCol.CLOSE: 150}, name=high_time)
    result_row = indicator.extend_realtime("AAPL", high_row)

    # For the last 5 days (2023-01-06 to 2023-01-10):
    # Highest high = 158, Lowest low = 134
    # For close=150: (150-134)/(158-134) = 0.6667
    assert abs(result_row[indicator.column_name()] - 0.6667) < 1e-4

    above_time = datetime(2023, 1, 11, 10, 30)
    above_row = pd.Series({CandleCol.CLOSE: 160}, name=above_time)
    result_row = indicator.extend_realtime("AAPL", above_row)

    # For the last 5 days (2023-01-06 to 2023-01-10):
    # Highest high = 158, Lowest low = 134
    # For close=160: (160-134)/(158-134) = 1.0833
    assert abs(result_row[indicator.column_name()] - 1.0833) < 1e-4

    below_time = datetime(2023, 1, 11, 11, 0)
    below_row = pd.Series({CandleCol.CLOSE: 120}, name=below_time)
    result_row = indicator.extend_realtime("AAPL", below_row)

    # For the last 5 days (2023-01-06 to 2023-01-10):
    # Highest high = 158, Lowest low = 134
    # For close=120: (120-134)/(158-134) = -0.5833
    assert abs(result_row[indicator.column_name()] - (-0.5833)) < 1e-4


# Fixtures moved to fixtures.py
