from datetime import UTC, datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import polars as pl
import arrow

# Using ZoneInfo to properly handle Daylight Saving Time for the Eastern timezone.
EST_TIMEZONE = timezone(timedelta(hours=-5)) #fixed offset for EST
EASTERN_TIMEZONE = ZoneInfo("America/New_York")


def to_offset_aware(dt: datetime, offset_seconds: int) -> datetime:
    """Converts a naive datetime object to an aware datetime object in the offset timezone.

    Args:
      dt: The naive datetime object to convert.
      offset_seconds: The offset from UTC in seconds.

    Returns:
      An aware datetime object in the offset timezone.
    """
    return arrow.get(dt).to(timezone(timedelta(seconds=offset_seconds))).datetime


def to_eastern_aware(dt: datetime) -> datetime:
    """Converts a naive datetime object to an aware datetime object in the US/Eastern timezone.

    It's assumed the naive datetime is in UTC.

    Args:
      dt: The naive datetime object to convert (assumed to be in UTC).

    Returns:
      An aware datetime object in the US/Eastern timezone.
    """
    return arrow.get(dt).to(EASTERN_TIMEZONE).datetime

def to_est_aware(dt: datetime) -> datetime:
    """Converts a naive datetime object to an aware datetime object in the US/Eastern timezone.

    It's assumed the naive datetime is in UTC.

    Args:
      dt: The naive datetime object to convert (assumed to be in UTC).

    Returns:
      An aware datetime object in the US/Eastern timezone.
    """
    return arrow.get(dt).to(EST_TIMEZONE).datetime


def to_utc_aware(dt: datetime) -> datetime:
    """Converts a naive datetime object to an aware datetime object in UTC.

    If the datetime is already aware, it's converted to UTC.

    Args:
      dt: The datetime object to convert.

    Returns:
      An aware datetime object in UTC.
    """
    return arrow.get(dt).to(UTC).datetime


def get_current_event_time_str() -> str:
    """
    Gets the current time in US/Eastern as a string.

    Returns:
        str: The current time in US/Eastern as a string.
    """
    return arrow.now(EASTERN_TIMEZONE).format("YYYY/MM/DD hh:mm A")


def get_current_event_time() -> datetime:
    """
    Gets the current time in the US/Eastern timezone.

    Returns:
        datetime: The current time in the US/Eastern timezone.
    """
    return arrow.now(EASTERN_TIMEZONE).datetime


def get_current_time_str() -> str:
    """
    Gets the current time in UTC as a string.

    Returns:
        str: The current time in UTC as a string in ISO format.
    """
    return arrow.utcnow().isoformat()


def get_current_time() -> datetime:
    """
    Gets the current time in UTC.

    Returns:
        datetime: The current time in UTC.
    """
    return arrow.utcnow().datetime

def pl_col_utc(name, *more_names):
    """
    Convert a pandas column name to a Polars column name by replacing spaces with underscores.

    Args:
        col: The pandas column name.

    Returns:
        The Polars column name.
    """
    return pl.col(name, *more_names).dt.cast_time_unit('us').dt.replace_time_zone("UTC")