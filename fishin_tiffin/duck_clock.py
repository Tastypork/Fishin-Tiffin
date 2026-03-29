"""Shared wall-clock helper for duck game timing."""

from datetime import datetime, timezone


def utc_ts() -> int:
    return int(datetime.now(timezone.utc).timestamp())
