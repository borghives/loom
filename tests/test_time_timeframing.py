import pytest
from datetime import datetime, timezone, timedelta

from loom.time.timeframing import (
    TimeFrameResolution,
    TimeFrame,
    HourlyFrame,
    DailyFrame,
    WeeklyFrame,
    MonthlyFrame,
    QuarterlyFrame,
    YearlyFrame,
    align_to_human_timeframe,
)

# A fixed point in time for consistent testing
MOMENT = datetime(2023, 10, 26, 10, 30, 0, tzinfo=timezone.utc)
PST = timezone(timedelta(hours=-8))


@pytest.mark.parametrize(
    "key, frame_type, resolution",
    [
        ("hourly", HourlyFrame, TimeFrameResolution.hourly),
        ("daily", DailyFrame, TimeFrameResolution.daily),
        ("weekly", WeeklyFrame, TimeFrameResolution.weekly),
        ("monthly", MonthlyFrame, TimeFrameResolution.monthly),
        ("quarterly", QuarterlyFrame, TimeFrameResolution.quarterly),
        ("yearly", YearlyFrame, TimeFrameResolution.yearly),
        ("none", TimeFrame, TimeFrameResolution.none),
    ],
)
def test_time_frame_resolution_mapping(key, frame_type, resolution):
    """Tests mapping from names and types to TimeFrameResolution."""
    assert TimeFrameResolution.from_name(key) == resolution
    assert TimeFrameResolution.from_type(frame_type) == resolution


def test_time_frame_resolution_from_name_invalid():
    """Tests the default case for from_name with an invalid name."""
    assert (
        TimeFrameResolution.from_name("invalid", default=TimeFrameResolution.none)
        == TimeFrameResolution.none
    )


class TestTimeFrame:
    def test_init_with_floor_and_ceiling(self):
        floor = datetime(2023, 1, 1, tzinfo=timezone.utc)
        ceiling = datetime(2023, 1, 2, tzinfo=timezone.utc)
        frame = TimeFrame.create(floor=floor, ceiling=ceiling)
        assert frame.floor == floor
        assert frame.ceiling == ceiling

    def test_init_with_moment(self):
        # This will just create a default timeframe with the moment as floor
        frame = TimeFrame.create(moment=MOMENT)
        assert frame.floor == MOMENT
        # Default ceiling is floor + 1 hour
        assert frame.ceiling == MOMENT + timedelta(hours=1)

    def test_init_floor_after_ceiling_raises_error(self):
        with pytest.raises(ValueError, match="Floor must be before ceiling"):
            TimeFrame.create(
                floor=datetime(2023, 1, 2, tzinfo=timezone.utc),
                ceiling=datetime(2023, 1, 1, tzinfo=timezone.utc),
            )

    def test_init_mismatched_ceiling_raises_error(self):
        class TestFrame(TimeFrame):
            @classmethod
            def calculate_ceiling(cls, moment: datetime) -> datetime:
                return moment.replace(
                    minute=0, second=0, microsecond=0
                ) + timedelta(hours=1)

        with pytest.raises(
            ValueError, match="Ceiling does not match the calculated frame"
        ):
            TestFrame.create(
                floor=datetime(2023, 1, 1, 10, 0, 0, tzinfo=timezone.utc),
                ceiling=datetime(
                    2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc
                ),  # Mismatched ceiling
            )

    def test_get_floor_ceiling_with_offset(self):
        frame = DailyFrame.create(moment=MOMENT, tzone=PST)

        # Internally, floor/ceiling are UTC
        assert frame.floor.tzinfo == timezone.utc
        assert frame.floor == datetime(2023, 10, 26, 8, 0, 0, tzinfo=timezone.utc)

        # get_floor/get_ceiling should apply the offset
        local_floor = frame.get_floor()
        assert local_floor.tzinfo == PST
        assert local_floor.hour == 0
        assert local_floor.day == 26

        local_ceiling = frame.get_ceiling()
        assert local_ceiling.tzinfo == PST
        assert local_ceiling.hour == 0
        assert local_ceiling.day == 27

    def test_frame_navigation(self):
        frame = DailyFrame.create(moment=MOMENT)

        next_frame = frame.get_next_frame()
        assert next_frame.floor == frame.ceiling
        assert isinstance(next_frame, DailyFrame)

        prev_frame = frame.get_previous_frame()
        assert prev_frame.ceiling == frame.floor
        assert isinstance(prev_frame, DailyFrame)

        prev_5_frame = frame.get_previous_x_frame(5)
        expected_floor = frame.floor - timedelta(days=5)
        assert prev_5_frame.floor == expected_floor


class TestTimeFrames:
    @pytest.mark.parametrize(
        "frame_class, moment, expected_floor, expected_ceiling",
        [
            # Standard cases
            (
                HourlyFrame,
                MOMENT,
                datetime(2023, 10, 26, 10, 0, 0, tzinfo=timezone.utc),
                datetime(2023, 10, 26, 11, 0, 0, tzinfo=timezone.utc),
            ),
            (
                DailyFrame,
                MOMENT,
                datetime(2023, 10, 26, 0, 0, 0, tzinfo=timezone.utc),
                datetime(2023, 10, 27, 0, 0, 0, tzinfo=timezone.utc),
            ),
            (
                WeeklyFrame,  # Week starts on Monday, moment is a Thursday
                MOMENT,
                datetime(2023, 10, 23, 0, 0, 0, tzinfo=timezone.utc),
                datetime(2023, 10, 30, 0, 0, 0, tzinfo=timezone.utc),
            ),
            (
                MonthlyFrame,
                MOMENT,
                datetime(2023, 10, 1, 0, 0, 0, tzinfo=timezone.utc),
                datetime(2023, 11, 1, 0, 0, 0, tzinfo=timezone.utc),
            ),
            (
                QuarterlyFrame,  # October is in Q4
                MOMENT,
                datetime(2023, 10, 1, 0, 0, 0, tzinfo=timezone.utc),
                datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
            ),
            (
                YearlyFrame,
                MOMENT,
                datetime(2023, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
                datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
            ),
            # Edge cases
            (
                MonthlyFrame,  # Leap year
                datetime(2024, 2, 15, tzinfo=timezone.utc),
                datetime(2024, 2, 1, 0, 0, 0, tzinfo=timezone.utc),
                datetime(2024, 3, 1, 0, 0, 0, tzinfo=timezone.utc),
            ),
            (
                WeeklyFrame,  # Sunday
                datetime(2023, 10, 29, 15, 0, 0, tzinfo=timezone.utc),
                datetime(2023, 10, 23, 0, 0, 0, tzinfo=timezone.utc),
                datetime(2023, 10, 30, 0, 0, 0, tzinfo=timezone.utc),
            ),
            (
                QuarterlyFrame,  # Q1
                datetime(2023, 2, 15, tzinfo=timezone.utc),
                datetime(2023, 1, 1, tzinfo=timezone.utc),
                datetime(2023, 4, 1, tzinfo=timezone.utc),
            ),
            (
                QuarterlyFrame,  # Q2
                datetime(2023, 5, 15, tzinfo=timezone.utc),
                datetime(2023, 4, 1, tzinfo=timezone.utc),
                datetime(2023, 7, 1, tzinfo=timezone.utc),
            ),
        ],
    )
    def test_frame_calculations(
        self, frame_class, moment, expected_floor, expected_ceiling
    ):
        """Tests that floor and ceiling are calculated correctly for all frame types."""
        frame = frame_class.create(moment=moment)
        assert frame.floor == expected_floor
        assert frame.ceiling == expected_ceiling


@pytest.mark.parametrize(
    "frame_class",
    [YearlyFrame, QuarterlyFrame, MonthlyFrame, WeeklyFrame, DailyFrame, HourlyFrame],
)
def test_align_to_human_timeframe_standard(frame_class):
    """Tests aligning a standard frame finds the correct type."""
    frame = frame_class.create(moment=MOMENT)
    aligned_frame = align_to_human_timeframe(frame.floor, frame.ceiling, frame.alignment_offset_seconds)
    assert isinstance(aligned_frame, frame_class)
    assert aligned_frame.floor == frame.floor
    assert aligned_frame.ceiling == frame.ceiling


def test_align_to_human_timeframe_offset():
    """Tests aligning a frame with a timezone offset."""
    monthly_pst = MonthlyFrame.create(moment=MOMENT, tzone=PST)
    aligned_monthly = align_to_human_timeframe(
        monthly_pst.floor, monthly_pst.ceiling, monthly_pst.alignment_offset_seconds
    )
    assert isinstance(aligned_monthly, MonthlyFrame)
    assert (
        aligned_monthly.alignment_offset_seconds
        == PST.utcoffset(None).total_seconds()
    )


def test_align_to_human_timeframe_non_standard():
    """Tests that a non-standard interval returns a base TimeFrame."""
    floor = datetime(2023, 1, 1, tzinfo=timezone.utc)
    ceiling = datetime(2023, 1, 1, 12, 30, tzinfo=timezone.utc)  # 12.5 hours
    non_standard = align_to_human_timeframe(floor, ceiling)
    assert type(non_standard) is TimeFrame


@pytest.mark.parametrize(
    "current_time, expected_passed, expected_in_frame, expected_elapsed",
    [
        (datetime(2023, 10, 26, 9, 0, 0, tzinfo=timezone.utc), False, False, -1.0),
        (datetime(2023, 10, 26, 10, 30, 0, tzinfo=timezone.utc), False, True, 0.5),
        (datetime(2023, 10, 26, 12, 0, 0, tzinfo=timezone.utc), True, False, 1.0),
    ],
)
def test_time_status(
    monkeypatch, current_time, expected_passed, expected_in_frame, expected_elapsed
):
    """Tests has_passed(), is_in_frame(), and elapsed() at different times."""
    # Frame is 2023-10-26 10:00 to 11:00 UTC
    frame = HourlyFrame.create(moment=MOMENT)
    monkeypatch.setattr("loom.time.timeframing.get_current_time", lambda: current_time)

    assert frame.has_passed() == expected_passed
    assert frame.is_in_frame() == expected_in_frame
    assert frame.elapsed() == expected_elapsed