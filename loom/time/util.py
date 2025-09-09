from datetime import UTC, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

# Using ZoneInfo to properly handle Daylight Saving Time for the Eastern timezone.
# "EST" is a fixed offset, which is incorrect during EDT.
EASTERN_TIMEZONE = ZoneInfo("America/New_York")


def to_offset_aware(dt: datetime, offset_seconds: int) -> datetime:
    """Converts a naive datetime object to an aware datetime object in the offset timezone.

    Args:
      dt: The naive datetime object to convert.
      offset_seconds: The offset from UTC in seconds.

    Returns:
      An aware datetime object in the offset timezone.
    """

    offset_zone = timezone(timedelta(seconds=offset_seconds))

    if dt.tzinfo is not None:
        return dt.astimezone(offset_zone)
    else:
        return dt.replace(tzinfo=offset_zone)


def to_eastern_aware(dt: datetime) -> datetime:
    """Converts a naive datetime object to an aware datetime object in the US/Eastern timezone.

    It's assumed the naive datetime is in UTC.

    Args:
      dt: The naive datetime object to convert (assumed to be in UTC).

    Returns:
      An aware datetime object in the US/Eastern timezone.
    """

    return to_utc_aware(dt).astimezone(EASTERN_TIMEZONE)


def to_utc_aware(dt: datetime) -> datetime:
    """Converts a naive datetime object to an aware datetime object in UTC.

    If the datetime is already aware, it's converted to UTC.

    Args:
      dt: The datetime object to convert.

    Returns:
      An aware datetime object in UTC.
    """

    if dt.tzinfo is not None:
        return dt.astimezone(UTC)
    else:
        return dt.replace(tzinfo=UTC)


def get_current_event_time_str() -> str:
    """
    Gets the current time in US/Eastern as a string.

    Returns:
        str: The current time in US/Eastern as a string.
    """
    return get_current_event_time().strftime("%Y/%m/%d %I:%M %p")


def get_current_event_time() -> datetime:
    """
    Gets the current time in the US/Eastern timezone.

    Returns:
        datetime: The current time in the US/Eastern timezone.
    """
    return datetime.now(EASTERN_TIMEZONE)


def get_current_time_str() -> str:
    """
    Gets the current time in UTC as a string.

    Returns:
        str: The current time in UTC as a string in ISO format.
    """
    return get_current_time().isoformat()


def get_current_time() -> datetime:
    """
    Gets the current time in UTC.

    Returns:
        datetime: The current time in UTC.
    """
    return datetime.now(UTC)
