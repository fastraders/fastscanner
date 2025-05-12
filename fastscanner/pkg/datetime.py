import re
from datetime import date
from zoneinfo import ZoneInfo

LOCAL_TIMEZONE_STR = "America/New_York"
LOCAL_TIMEZONE = ZoneInfo(LOCAL_TIMEZONE_STR)


def split_freq(freq: str) -> tuple[int, str]:
    match = re.match(r"(\d+)(\w+)", freq)
    if match is None:
        raise ValueError(f"Invalid frequency: {freq}")
    mult = int(match.groups()[0])
    unit = match.groups()[1].lower()
    return mult, unit
