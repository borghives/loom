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
MOMENT_PST = MOMENT.astimezone(PST)


def test_time_frame_resolution_from_name():
    assert TimeFrameResolution.from_name("hourly") == TimeFrameResolution.hourly
    assert TimeFrameResolution.from_name("daily") == TimeFrameResolution.daily
    assert TimeFrameResolution.from_name("weekly") == TimeFrameResolution.weekly
    assert TimeFrameResolution.from_name("monthly") == TimeFrameResolution.monthly
    assert TimeFrameResolution.from_name("quarterly") == TimeFrameResolution.quarterly
    assert TimeFrameResolution.from_name("yearly") == TimeFrameResolution.yearly
    assert (
        TimeFrameResolution.from_name("invalid", default=TimeFrameResolution.none)
        == TimeFrameResolution.none
    )


def test_time_frame_resolution_from_type():
    assert TimeFrameResolution.from_type(HourlyFrame) == TimeFrameResolution.hourly
    assert TimeFrameResolution.from_type(DailyFrame) == TimeFrameResolution.daily
    assert TimeFrameResolution.from_type(WeeklyFrame) == TimeFrameResolution.weekly
    assert TimeFrameResolution.from_type(MonthlyFrame) == TimeFrameResolution.monthly
    assert TimeFrameResolution.from_type(QuarterlyFrame) == TimeFrameResolution.quarterly
    assert TimeFrameResolution.from_type(YearlyFrame) == TimeFrameResolution.yearly
    assert TimeFrameResolution.from_type(TimeFrame) == TimeFrameResolution.none


class TestTimeFrame:
    def test_init_with_floor_and_ceiling(self):
        floor = datetime(2023, 1, 1, tzinfo=timezone.utc)
        ceiling = datetime(2023, 1, 2, tzinfo=timezone.utc)
        frame = TimeFrame(floor=floor, ceiling=ceiling)
        assert frame.floor == floor
        assert frame.ceiling == ceiling

    def test_init_with_moment(self):
        # This will just create a default timeframe with the moment as floor
        frame = TimeFrame(moment=MOMENT)
        assert frame.floor == MOMENT
        # Default ceiling is floor + 1 hour
        assert frame.ceiling == MOMENT + timedelta(hours=1)

    def test_init_floor_after_ceiling_raises_error(self):
        with pytest.raises(ValueError, match="Floor must be before ceiling"):
            TimeFrame(
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
            TestFrame(
                floor=datetime(2023, 1, 1, 10, 0, 0, tzinfo=timezone.utc),
                ceiling=datetime(
                    2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc
                ),  # Mismatched ceiling
            )

    def test_get_floor_ceiling_with_offset(self):
        offset_seconds = -8 * 3600  # PST
        tzone = timezone(timedelta(seconds=offset_seconds))
        frame = DailyFrame(moment=MOMENT, tzone=tzone)

        # Internally, floor/ceiling are UTC
        assert frame.floor.tzinfo == timezone.utc
        assert frame.floor == datetime(2023, 10, 26, 8, 0, 0, tzinfo=timezone.utc)

        # get_floor/get_ceiling should apply the offset
        local_floor = frame.get_floor()
        assert local_floor.tzinfo == tzone
        assert local_floor.hour == 0
        assert local_floor.day == 26

        local_ceiling = frame.get_ceiling()
        assert local_ceiling.tzinfo == tzone
        assert local_ceiling.hour == 0
        assert local_ceiling.day == 27

    def test_frame_navigation(self):
        frame = DailyFrame(moment=MOMENT)

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
                WeeklyFrame,  # Week starts on Monday
                MOMENT,  # This is a Thursday
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
                # Test MonthlyFrame with leap year
                MonthlyFrame,
                datetime(2024, 2, 15, tzinfo=timezone.utc),
                datetime(2024, 2, 1, 0, 0, 0, tzinfo=timezone.utc),
                datetime(2024, 3, 1, 0, 0, 0, tzinfo=timezone.utc),
            ),
            (
                QuarterlyFrame,
                MOMENT,  # October is in Q4
                datetime(2023, 10, 1, 0, 0, 0, tzinfo=timezone.utc),
                datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
            ),
            (
                YearlyFrame,
                MOMENT,
                datetime(2023, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
                datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
            ),
        ],
    )
    def test_frame_calculations(
        self, frame_class, moment, expected_floor, expected_ceiling
    ):
        frame = frame_class(moment=moment)
        assert frame.floor == expected_floor
        assert frame.ceiling == expected_ceiling

    def test_weekly_frame_edge_case(self):
        # Test a Sunday
        moment = datetime(2023, 10, 29, 15, 0, 0, tzinfo=timezone.utc)  # Sunday
        frame = WeeklyFrame(moment=moment)
        assert frame.floor == datetime(2023, 10, 23, 0, 0, 0, tzinfo=timezone.utc)
        assert frame.ceiling == datetime(2023, 10, 30, 0, 0, 0, tzinfo=timezone.utc)

    def test_quarterly_frame_edge_cases(self):
        # Q1
        q1_moment = datetime(2023, 2, 15, tzinfo=timezone.utc)
        q1_frame = QuarterlyFrame(moment=q1_moment)
        assert q1_frame.floor == datetime(2023, 1, 1, tzinfo=timezone.utc)
        assert q1_frame.ceiling == datetime(2023, 4, 1, tzinfo=timezone.utc)
        # Q2
        q2_moment = datetime(2023, 5, 15, tzinfo=timezone.utc)
        q2_frame = QuarterlyFrame(moment=q2_moment)
        assert q2_frame.floor == datetime(2023, 4, 1, tzinfo=timezone.utc)
        assert q2_frame.ceiling == datetime(2023, 7, 1, tzinfo=timezone.utc)


def test_align_to_human_timeframe():
    # Test Yearly
    yearly = YearlyFrame(moment=MOMENT)
    aligned_yearly = align_to_human_timeframe(yearly.floor, yearly.ceiling)
    assert isinstance(aligned_yearly, YearlyFrame)
    assert aligned_yearly.floor == yearly.floor
    assert aligned_yearly.ceiling == yearly.ceiling

    # Test Daily
    daily = DailyFrame(moment=MOMENT)
    aligned_daily = align_to_human_timeframe(daily.floor, daily.ceiling)
    assert isinstance(aligned_daily, DailyFrame)

    # Test with offset
    offset_seconds = -7 * 3600
    tzone = timezone(timedelta(seconds=offset_seconds))
    monthly_pst = MonthlyFrame(moment=MOMENT, tzone=tzone)
    aligned_monthly = align_to_human_timeframe(
        monthly_pst.floor, monthly_pst.ceiling, monthly_pst.alignment_offset_seconds
    )
    assert isinstance(aligned_monthly, MonthlyFrame)
    assert aligned_monthly.alignment_offset_seconds == offset_seconds

    # Test non-standard frame
    floor = datetime(2023, 1, 1, tzinfo=timezone.utc)
    ceiling = datetime(2023, 1, 1, 12, 30, tzinfo=timezone.utc)  # 12.5 hours
    non_standard = align_to_human_timeframe(floor, ceiling)
    assert type(non_standard) is TimeFrame


def test_has_passed_and_is_in_frame(monkeypatch):
    # Frame is 2023-10-26 10:00 to 11:00 UTC
    frame = HourlyFrame(moment=MOMENT)

    # Case 1: Current time is before the frame
    monkeypatch.setattr(
        "loom.time.timeframing.get_current_time",
        lambda: datetime(2023, 10, 26, 9, 0, 0, tzinfo=timezone.utc),
    )
    assert not frame.has_passed()
    assert not frame.is_in_frame()

    # Case 2: Current time is inside the frame
    monkeypatch.setattr(
        "loom.time.timeframing.get_current_time",
        lambda: datetime(2023, 10, 26, 10, 30, 0, tzinfo=timezone.utc),
    )
    assert not frame.has_passed()
    assert frame.is_in_frame()
    assert frame.elapsed() == 0.5

    # Case 3: Current time is after the frame
    monkeypatch.setattr(
        "loom.time.timeframing.get_current_time",
        lambda: datetime(2023, 10, 26, 12, 0, 0, tzinfo=timezone.utc),
    )
    assert frame.has_passed()
    assert not frame.is_in_frame()
    assert frame.elapsed() == 1.0
