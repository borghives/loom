from datetime import datetime, timedelta, timezone

from dateutil import tz

from loom.time.util import (
    EASTERN_TIMEZONE,
    to_eastern_aware,
    to_offset_aware,
    to_utc_aware,
    get_current_time,
    get_current_event_time,
)

# A naive datetime for testing
NAIVE_DT = datetime(2023, 10, 26, 10, 0, 0)

# An aware datetime in UTC for testing
UTC_AWARE_DT = datetime(2023, 10, 26, 10, 0, 0, tzinfo=timezone.utc)


def test_to_offset_aware():
    offset_seconds = -18000  # -5 hours
    offset_zone = timezone(timedelta(seconds=offset_seconds))

    # Test with naive datetime
    aware_dt = to_offset_aware(NAIVE_DT, offset_seconds)
    assert aware_dt.tzinfo is not None
    assert aware_dt.tzinfo == offset_zone

    # Test with already aware datetime
    aware_dt_from_utc = to_offset_aware(UTC_AWARE_DT, offset_seconds)
    assert aware_dt_from_utc.tzinfo == offset_zone
    assert aware_dt_from_utc.astimezone(timezone.utc) == UTC_AWARE_DT


def test_to_utc_aware():
    # Test with naive datetime
    utc_dt = to_utc_aware(NAIVE_DT)
    assert utc_dt.tzinfo == timezone.utc
    assert utc_dt.replace(tzinfo=None) == NAIVE_DT

    # Test with aware datetime in another timezone
    offset_zone = timezone(timedelta(hours=-4))
    aware_dt = NAIVE_DT.replace(tzinfo=offset_zone)
    utc_dt_from_aware = to_utc_aware(aware_dt)
    assert utc_dt_from_aware.tzinfo == timezone.utc
    assert utc_dt_from_aware == aware_dt.astimezone(timezone.utc)


def test_to_eastern_aware_handles_dst():
    # Test with a date in standard time (EST, UTC-5)
    winter_naive = datetime(2023, 1, 15, 12, 0, 0)
    eastern_dt_winter = to_eastern_aware(winter_naive)
    assert eastern_dt_winter.tzinfo == EASTERN_TIMEZONE
    assert eastern_dt_winter.utcoffset() == timedelta(hours=-5)
    assert eastern_dt_winter.strftime('%Z') == 'EST'

    # Test with a date in daylight saving time (EDT, UTC-4)
    summer_naive = datetime(2023, 7, 15, 12, 0, 0)
    eastern_dt_summer = to_eastern_aware(summer_naive)
    assert eastern_dt_summer.tzinfo == EASTERN_TIMEZONE
    assert eastern_dt_summer.utcoffset() == timedelta(hours=-4)
    assert eastern_dt_summer.strftime('%Z') == 'EDT'

    # Test with an already aware UTC datetime
    utc_aware_summer = summer_naive.replace(tzinfo=timezone.utc)
    eastern_from_aware = to_eastern_aware(utc_aware_summer)
    assert eastern_from_aware.astimezone(timezone.utc) == utc_aware_summer
    assert eastern_from_aware.utcoffset() == timedelta(hours=-4)


def test_get_current_time():
    current_time = get_current_time()
    assert current_time.tzinfo is not None
    assert current_time.tzinfo == tz.tzutc()


def test_get_current_event_time():
    current_event_time = get_current_event_time()
    assert current_event_time.tzinfo is not None
    assert current_event_time.tzinfo == EASTERN_TIMEZONE
