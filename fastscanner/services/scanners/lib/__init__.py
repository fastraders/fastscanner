from contextvars import ContextVar
from typing import Any, Hashable

from ..ports import Scanner, ScannerRealtime
from .day2 import Day2GapScanner
from .gap import ATRGapDownScanner, ATRGapUpScanner
from .parabolic import (
    ATRParabolicDownScanner,
    ATRParabolicUpScanner,
    DailyATRParabolicDownScanner,
    DailyATRParabolicUpScanner,
)
from .range_gap import HighRangeGapUpScanner, LowRangeGapDownScanner
from .smallcap import SmallCapUpScanner

_scanners: list[type[Scanner]] = [
    ATRGapDownScanner,
    ATRGapUpScanner,
    ATRParabolicDownScanner,
    ATRParabolicUpScanner,
    DailyATRParabolicDownScanner,
    DailyATRParabolicUpScanner,
    HighRangeGapUpScanner,
    LowRangeGapDownScanner,
    SmallCapUpScanner,
    Day2GapScanner,
]
_scanners_realtime: list[type[ScannerRealtime]] = [
    ATRGapDownScanner,
    ATRGapUpScanner,
    ATRParabolicDownScanner,
    ATRParabolicUpScanner,
    HighRangeGapUpScanner,
    LowRangeGapDownScanner,
    SmallCapUpScanner,
]


class ScannersLibrary:
    _instance: ContextVar["ScannersLibrary"] = ContextVar("ScannersLibrary")

    def __init__(self):
        self._scanners: dict[str, type[Scanner]] = {}
        self._scanners_realtime: dict[str, type[ScannerRealtime]] = {}

    def get(self, type_: str, params: dict[str, Hashable]) -> Scanner:
        if type_ not in self._scanners:
            raise ValueError(f"Scanner {type_} not found.")
        scanner_class = self._scanners[type_]
        return scanner_class(**params)

    def get_realtime(self, type_: str, params: dict[str, Hashable]) -> ScannerRealtime:
        if type_ not in self._scanners:
            raise ValueError(f"Scanner {type_} not found.")
        scanner_class = self._scanners_realtime[type_]
        return scanner_class(**params)

    def register(self, scanner: type[Scanner]) -> None:
        self._scanners[scanner.type()] = scanner

    def register_realtime(self, scanner: type[ScannerRealtime]) -> None:
        self._scanners_realtime[scanner.type()] = scanner

    def is_realtime(self, type_: str) -> bool:
        return type_ in self._scanners_realtime

    @classmethod
    def instance(cls) -> "ScannersLibrary":
        try:
            return cls._instance.get()
        except LookupError:
            instance = cls()
            for scanner in _scanners:
                instance.register(scanner)
            for scanner in _scanners_realtime:
                instance.register_realtime(scanner)
            cls._instance.set(instance)
            return instance
