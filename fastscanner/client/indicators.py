"""Pydantic models for all indicators with to_params methods."""

from enum import Enum
from pydantic import BaseModel
from fastscanner.services.indicators.service import IndicatorParams


class CumulativeOperation(str, Enum):
    MIN = "min"
    MAX = "max"
    SUM = "sum"


# Candle indicators
class CumulativeDailyVolume(BaseModel):
    def to_params(self) -> IndicatorParams:
        return IndicatorParams(type_="cumulative_daily_volume", params={})


class PremarketCumulative(BaseModel):
    candle_col: str
    op: CumulativeOperation
    
    def to_params(self) -> IndicatorParams:
        return IndicatorParams(
            type_="premarket_cumulative", 
            params={"candle_col": self.candle_col, "op": self.op.value}
        )


class Cumulative(BaseModel):
    candle_col: str
    op: CumulativeOperation
    
    def to_params(self) -> IndicatorParams:
        return IndicatorParams(
            type_="cumulative", 
            params={"candle_col": self.candle_col, "op": self.op.value}
        )


class ATR(BaseModel):
    period: int
    freq: str
    
    def to_params(self) -> IndicatorParams:
        return IndicatorParams(
            type_="atr", 
            params={"period": self.period, "freq": self.freq}
        )


class PositionInRange(BaseModel):
    n_days: int
    
    def to_params(self) -> IndicatorParams:
        return IndicatorParams(
            type_="position_in_range", 
            params={"n_days": self.n_days}
        )


class DailyRolling(BaseModel):
    n_days: int
    operation: CumulativeOperation
    candle_col: str
    
    def to_params(self) -> IndicatorParams:
        return IndicatorParams(
            type_="daily_rolling",
            params={
                "n_days": self.n_days, 
                "operation": self.operation.value, 
                "candle_col": self.candle_col
            }
        )


class Gap(BaseModel):
    candle_col: str
    
    def to_params(self) -> IndicatorParams:
        return IndicatorParams(
            type_="gap", 
            params={"candle_col": self.candle_col}
        )


class ATRGap(BaseModel):
    period: int
    candle_col: str
    
    def to_params(self) -> IndicatorParams:
        return IndicatorParams(
            type_="atr_gap", 
            params={"period": self.period, "candle_col": self.candle_col}
        )


class Shift(BaseModel):
    candle_col: str
    shift: int
    
    def to_params(self) -> IndicatorParams:
        return IndicatorParams(
            type_="shift", 
            params={"candle_col": self.candle_col, "shift": self.shift}
        )


# Daily indicators
class PrevDay(BaseModel):
    candle_col: str
    
    def to_params(self) -> IndicatorParams:
        return IndicatorParams(
            type_="prev_day", 
            params={"candle_col": self.candle_col}
        )


class DailyGap(BaseModel):
    def to_params(self) -> IndicatorParams:
        return IndicatorParams(type_="daily_gap", params={})


class DailyATR(BaseModel):
    period: int
    
    def to_params(self) -> IndicatorParams:
        return IndicatorParams(
            type_="daily_atr", 
            params={"period": self.period}
        )


class DailyATRGap(BaseModel):
    period: int
    
    def to_params(self) -> IndicatorParams:
        return IndicatorParams(
            type_="daily_atr_gap", 
            params={"period": self.period}
        )


class ADR(BaseModel):
    period: int
    
    def to_params(self) -> IndicatorParams:
        return IndicatorParams(
            type_="adr", 
            params={"period": self.period}
        )


class ADV(BaseModel):
    period: int
    
    def to_params(self) -> IndicatorParams:
        return IndicatorParams(
            type_="adv", 
            params={"period": self.period}
        )


# Fundamental indicators
class DaysToEarnings(BaseModel):
    def to_params(self) -> IndicatorParams:
        return IndicatorParams(type_="days_to_earnings", params={})


class DaysFromEarnings(BaseModel):
    def to_params(self) -> IndicatorParams:
        return IndicatorParams(type_="days_from_earnings", params={})


class MarketCap(BaseModel):
    def to_params(self) -> IndicatorParams:
        return IndicatorParams(type_="market_cap", params={})