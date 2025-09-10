from datetime import datetime, timedelta, UTC, timezone
from enum import Enum
from typing import Any, Optional
from pydantic import BaseModel, Field, field_validator

from loom.time.util import get_current_time, to_offset_aware, to_utc_aware


class TimeFrameResolution(str, Enum):
    """
    An enumeration of the different time frame resolutions that are supported.
    """

    none = "none"
    hourly = "hourly"
    daily = "daily"
    weekly = "weekly"
    monthly = "monthly"
    quarterly = "quarterly"
    yearly = "yearly"

    def current(self, tzone: Optional[timezone] = None) -> "TimeFrame":
        """
        Gets the current time frame for the resolution.

        Args:
            tzone (Optional[timezone], optional): The timezone to use.
                Defaults to `None`.

        Returns:
            TimeFrame: The current time frame.
        """
        match self.value:
            case TimeFrameResolution.hourly:
                return HourlyFrame(tzone=tzone)
            case TimeFrameResolution.daily:
                return DailyFrame(tzone=tzone)
            case TimeFrameResolution.weekly:
                return WeeklyFrame(tzone=tzone)
            case TimeFrameResolution.monthly:
                return MonthlyFrame(tzone=tzone)
            case TimeFrameResolution.quarterly:
                return QuarterlyFrame(tzone=tzone)
            case TimeFrameResolution.yearly:
                return YearlyFrame(tzone=tzone)
            case _:
                return HourlyFrame(tzone=tzone)

    @classmethod
    def from_name(
        cls, name: str, default: Optional["TimeFrameResolution"] = None
    ) -> "TimeFrameResolution":
        """
        Gets a `TimeFrameResolution` from its name.

        Args:
            name (str): The name of the resolution.
            default (Optional["TimeFrameResolution"], optional): The default
                resolution to return if the name is not valid. Defaults to
                `None`.

        Returns:
            TimeFrameResolution: The `TimeFrameResolution`.
        """
        try:
            return TimeFrameResolution[name]
        except KeyError:
            if default:
                return default
            else:
                return TimeFrameResolution.none

    @classmethod
    def from_type(cls, type: type) -> "TimeFrameResolution":
        """
        Gets a `TimeFrameResolution` from a `TimeFrame` type.

        Args:
            type (type): The `TimeFrame` type.

        Returns:
            TimeFrameResolution: The `TimeFrameResolution`.
        """
        if type == HourlyFrame:
            return TimeFrameResolution.hourly
        if type == DailyFrame:
            return TimeFrameResolution.daily
        if type == WeeklyFrame:
            return TimeFrameResolution.weekly
        if type == MonthlyFrame:
            return TimeFrameResolution.monthly
        if type == QuarterlyFrame:
            return TimeFrameResolution.quarterly
        if type == YearlyFrame:
            return TimeFrameResolution.yearly
        return TimeFrameResolution.none


# UTC based time framing
class TimeFrame(BaseModel):
    """
    Base class for human concept of time frame (e.g. Hourly, Daily, Weekly,
    Monthly, Quarterly, Yearly).
    """

    floor: datetime = Field(description="inclusive bottom of the time frame")  # in utc
    ceiling: datetime = Field(description="exclude top of the time frame")  # in utc
    alignment_offset_seconds: int = Field(
        0, description="alignment offset as seconds positive east of UTC (negative west of UTC)."
    )  # offset from utc

    def __init__(
        self,
        floor: Optional[datetime] = None,
        ceiling: Optional[datetime] = None,
        alignment_offset_seconds: int = 0,
        moment: Optional[datetime] = None,
        tzone: Optional[timezone] = None,
    ):
        if floor and ceiling:
            if floor.tzinfo is None:
                floor = floor.replace(tzinfo=timezone.utc)

            if ceiling.tzinfo is None:
                ceiling = ceiling.replace(tzinfo=timezone.utc)

            if alignment_offset_seconds:
                floor = floor.astimezone(
                    timezone(timedelta(seconds=alignment_offset_seconds))
                )
                ceiling = ceiling.astimezone(
                    timezone(timedelta(seconds=alignment_offset_seconds))
                )
            else:
                floor = floor.astimezone(UTC)
                ceiling = ceiling.astimezone(UTC)

            if floor > ceiling:
                raise ValueError("Floor must be before ceiling")

        else:
            if moment is None:
                moment = get_current_time()

            if moment.tzinfo is None:
                moment = moment.replace(tzinfo=timezone.utc)

            if tzone:
                moment = moment.astimezone(tzone)

            moment_utc_offset = moment.utcoffset()

            if moment_utc_offset:
                alignment_offset_seconds = int(moment_utc_offset.total_seconds())

            floor = self.calculate_floor(moment)
            ceiling = self.calculate_ceiling(moment)
            if not ceiling:
                ceiling = floor + timedelta(hours=1)

        calculated_ceiling = self.calculate_ceiling(floor)
        if calculated_ceiling and calculated_ceiling != ceiling:
            raise ValueError("Ceiling does not match the calculated frame")

        super().__init__(
            floor=floor,
            ceiling=ceiling,
            alignment_offset_seconds=alignment_offset_seconds,
        )

    @field_validator("floor")
    def check_floor(cls, v):
        return to_utc_aware(v)

    @field_validator("ceiling")
    def check_ceiling(cls, v):
        return to_utc_aware(v)

    @classmethod
    def calculate_floor(cls, moment: datetime) -> datetime:
        return moment  # default to reflect back the moment.

    @classmethod
    def calculate_ceiling(cls, moment: datetime) -> Optional[datetime]:
        return None

    @classmethod
    def get_inner_frame_type(cls):
        return None

    def has_passed(self) -> bool:
        """
        Checks if the time frame has passed.

        Returns:
            bool: `True` if the time frame has passed, `False` otherwise.
        """
        return self.ceiling <= get_current_time()

    def is_in_frame(self) -> bool:
        """
        Checks if the current time is within the time frame.

        Returns:
            bool: `True` if the current time is within the time frame, `False`
                otherwise.
        """
        current = get_current_time()
        return current >= self.floor and current < self.ceiling

    def elapsed(self) -> float:
        """
        Gets the percentage of the time frame that has elapsed.

        Returns:
            float: The percentage of the time frame that has elapsed.
        """
        if self.has_passed():
            return 1.0
        return (
            get_current_time() - self.floor
        ).total_seconds() / self.get_interval().total_seconds()

    def get_pretty_name(self) -> str:
        """
        Gets a pretty name for the time frame.

        Returns:
            str: A pretty name for the time frame.
        """
        return "Timeframe"

    def get_pretty_value(self) -> str:
        """
        Gets a pretty value for the time frame.

        Returns:
            str: A pretty value for the time frame.
        """
        floor = self.get_floor()
        ceiling = self.get_ceiling()
        return f"{floor.strftime('%Y-%m-%d')} - {ceiling.strftime('%Y-%m-%d') if ceiling else '__'}"

    def get_next_frame(self) -> "TimeFrame":
        """
        Gets the next time frame.

        Returns:
            TimeFrame: The next time frame.
        """
        return type(self)(moment=self.get_ceiling())

    def get_previous_frame(self) -> "TimeFrame":
        """
        Gets the previous time frame.

        Returns:
            TimeFrame: The previous time frame.
        """
        return type(self)(moment=self.get_floor() - timedelta(hours=1))

    def get_previous_x_frame(self, x: int) -> "TimeFrame":
        """
        Gets the x-th previous time frame.

        Args:
            x (int): The number of time frames to go back.

        Returns:
            TimeFrame: The x-th previous time frame.
        """
        window = self
        for _ in range(x):
            window = window.get_previous_frame()
        return window

    def get_interval(self) -> timedelta:
        """
        Gets the interval of the time frame.

        Returns:
            timedelta: The interval of the time frame.
        """
        upper_bound = self.ceiling if self.ceiling else get_current_time()
        return upper_bound - self.floor

    def get_floor(self) -> datetime:
        """
        Gets the floor of the time frame.

        Returns:
            datetime: The floor of the time frame.
        """
        return to_offset_aware(self.floor, self.alignment_offset_seconds)

    def get_ceiling(self) -> datetime:
        """
        Gets the ceiling of the time frame.

        Returns:
            datetime: The ceiling of the time frame.
        """
        return to_offset_aware(self.ceiling, self.alignment_offset_seconds)

    def __str__(self) -> str:
        return f"{self.floor} - {self.ceiling}"

    def model_dump(self, **kwargs) -> dict[str, Any]:
        return super().model_dump(by_alias=True, exclude_defaults=True)


class HourlyFrame(TimeFrame):
    """
    A time frame that represents an hour.
    """

    def __init__(
        self, moment: Optional[datetime] = None, tzone: Optional[timezone] = None, **kwargs
    ):
        super().__init__(moment=moment, tzone=tzone, **kwargs)

    @classmethod
    def calculate_floor(cls, moment: datetime) -> datetime:
        floor = moment.replace(minute=0, second=0, microsecond=0)
        return floor

    @classmethod
    def calculate_ceiling(cls, moment: datetime) -> datetime:
        return cls.calculate_floor(moment) + timedelta(hours=1)

    def get_pretty_name(self) -> str:
        return "Hourly"

    def get_pretty_value(self) -> str:
        floor = self.get_floor()
        ceiling = self.get_ceiling()
        return f"{floor.strftime('%m/%d')}: {floor.strftime('%I')} - {ceiling.strftime('%I %p') if ceiling else '___'}"


class DailyFrame(TimeFrame):
    """
    A time frame that represents a day.
    """

    def __init__(
        self, moment: Optional[datetime] = None, tzone: Optional[timezone] = None, **kwargs
    ):
        super().__init__(moment=moment, tzone=tzone, **kwargs)

    @classmethod
    def calculate_floor(cls, moment: datetime) -> datetime:
        floor = moment.replace(hour=0, minute=0, second=0, microsecond=0)
        return floor

    @classmethod
    def calculate_ceiling(cls, moment: datetime) -> datetime:
        return cls.calculate_floor(moment) + timedelta(days=1)

    @classmethod
    def get_inner_frame_type(cls):
        return HourlyFrame

    def get_pretty_name(self) -> str:
        return "Daily"

    def get_pretty_value(self) -> str:
        floor = self.get_floor()
        return f"{floor.strftime('%m/%d')}"


class WeeklyFrame(TimeFrame):
    """
    A time frame that represents a week.
    """

    def __init__(
        self, moment: Optional[datetime] = None, tzone: Optional[timezone] = None, **kwargs
    ):
        super().__init__(moment=moment, tzone=tzone, **kwargs)

    @classmethod
    def calculate_floor(cls, moment: datetime) -> datetime:
        floor = moment - timedelta(days=moment.weekday())
        return floor.replace(hour=0, minute=0, second=0, microsecond=0)

    @classmethod
    def calculate_ceiling(cls, moment):
        return cls.calculate_floor(moment) + timedelta(days=7)

    @classmethod
    def get_inner_frame_type(cls):
        return DailyFrame

    def get_pretty_name(self) -> str:
        return "Weekly"

    def get_pretty_value(self) -> str:
        floor = self.get_floor()
        ceiling = self.get_ceiling()
        return f"{floor.strftime('%m/%d')} - {ceiling.strftime('%m/%d') if ceiling else '__'}"


class MonthlyFrame(TimeFrame):
    """
    A time frame that represents a month.
    """

    def __init__(
        self, moment: Optional[datetime] = None, tzone: Optional[timezone] = None, **kwargs
    ):
        super().__init__(moment=moment, tzone=tzone, **kwargs)

    @classmethod
    def calculate_floor(cls, moment: datetime) -> datetime:
        return moment.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    @classmethod
    def calculate_ceiling(cls, moment):
        oneMoreDayThanTheLongestMonth = timedelta(days=32)
        # non inclusive next day of the month
        someDayNextMonth = cls.calculate_floor(moment) + oneMoreDayThanTheLongestMonth
        # change to 1st day of the next month and return
        return someDayNextMonth.replace(day=1)

    @classmethod
    def get_inner_frame_type(cls):
        return WeeklyFrame

    def get_pretty_name(self) -> str:
        return "Monthly"

    def get_pretty_value(self) -> str:
        floor = self.get_floor()
        return f"{floor.strftime('%b %Y')}"


class QuarterlyFrame(TimeFrame):
    """
    A time frame that represents a quarter.
    """

    def __init__(
        self, moment: Optional[datetime] = None, tzone: Optional[timezone] = None, **kwargs
    ):
        super().__init__(moment=moment, tzone=tzone, **kwargs)

    @classmethod
    def calculate_floor(cls, moment: datetime) -> datetime:
        quarter = (moment.month - 1) // 3 + 1
        return moment.replace(
            month=(quarter - 1) * 3 + 1, day=1, hour=0, minute=0, second=0, microsecond=0
        )

    @classmethod
    def calculate_ceiling(cls, moment: datetime) -> datetime:
        oneMoreDayThanTheLongestQuarter = timedelta(days=93)
        # non inclusive next day of the month
        someDayNextQuarter = (
            cls.calculate_floor(moment) + oneMoreDayThanTheLongestQuarter
        )
        # change to 1st day of the next month and return
        return someDayNextQuarter.replace(day=1)

    @classmethod
    def get_inner_frame_type(cls):
        return MonthlyFrame

    def get_pretty_name(self) -> str:
        return "Quarterly"

    def get_pretty_value(self) -> str:
        floor = self.get_floor()
        return f"Q{(floor.month - 1) // 3 + 1} {floor.year}"


class YearlyFrame(TimeFrame):
    """
    A time frame that represents a year.
    """

    def __init__(
        self, moment: Optional[datetime] = None, tzone: Optional[timezone] = None, **kwargs
    ):
        super().__init__(moment=moment, tzone=tzone, **kwargs)

    @classmethod
    def calculate_floor(cls, moment: datetime) -> datetime:
        return moment.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)

    @classmethod
    def calculate_ceiling(cls, moment: datetime) -> datetime:
        someDayNextYear = cls.calculate_floor(moment) + timedelta(days=366)
        return someDayNextYear.replace(day=1)

    @classmethod
    def get_inner_frame_type(cls):
        return QuarterlyFrame

    def get_pretty_name(self) -> str:
        return "Yearly"

    def get_pretty_value(self) -> str:
        floor = self.get_floor()
        return f"{floor.year}"


def align_to_human_timeframe(
    floor: datetime, ceiling: datetime, alignment_offset_seconds: int = 0
) -> TimeFrame:
    """
    Aligns a given floor and ceiling to the most appropriate `TimeFrame` type.

    Args:
        floor (datetime): The floor of the time frame.
        ceiling (datetime): The ceiling of the time frame.
        alignment_offset_seconds (int, optional): The alignment offset in
            seconds. Defaults to `0`.

    Returns:
        TimeFrame: The most appropriate `TimeFrame` type.
    """
    align_type = YearlyFrame
    while align_type is not None:
        potential = align_type(moment=to_offset_aware(floor, alignment_offset_seconds))
        if (
            potential.floor == floor
            and potential.ceiling == ceiling
            and potential.alignment_offset_seconds == alignment_offset_seconds
        ):
            return potential
        align_type = align_type.get_inner_frame_type()

    return TimeFrame(
        floor=floor, ceiling=ceiling, alignment_offset_seconds=alignment_offset_seconds
    )
