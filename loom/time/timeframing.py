from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Optional
from zoneinfo import ZoneInfo

import arrow
from pydantic import BaseModel, Field, model_validator

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

    def current(self, tzone: timezone | ZoneInfo = timezone.utc) -> "TimeFrame":
        """
        Gets the current time frame for the resolution.

        Args:
            tzone (Optional[timezone | ZoneInfo], optional): The timezone to use.
                Defaults to `None`.

        Returns:
            TimeFrame: The current time frame.
        """
        frame_class = _RESOLUTION_TO_FRAME_CLASS_MAP.get(self, HourlyFrame)
        return frame_class.create(tzone=tzone)

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
        return _FRAME_CLASS_TO_RESOLUTION_MAP.get(type, TimeFrameResolution.none)


# UTC based time framing
class TimeFrame(BaseModel):
    """
    Base class for human concept of time frame (e.g. Hourly, Daily, Weekly,
    Monthly, Quarterly, Yearly).
    """

    floor: datetime = Field(description="inclusive bottom of the time frame")
    ceiling: datetime = Field(description="exclusive top of the time frame")
    alignment_offset_seconds: int = Field(
        0,
        description="alignment offset as seconds positive east of UTC (negative west of UTC).",
    )

    @classmethod
    def create(
        cls,
        moment: Optional[datetime] = None,
        tzone: timezone | ZoneInfo = timezone.utc,
        floor: Optional[datetime] = None,
        ceiling: Optional[datetime] = None,
        alignment_offset_seconds: Optional[int] = None,
    ) -> "TimeFrame":
        if floor and ceiling:
            if alignment_offset_seconds is None:
                # If no offset is provided, assume UTC.
                alignment_offset_seconds = 0
            return cls(
                floor=to_utc_aware(floor),
                ceiling=to_utc_aware(ceiling),
                alignment_offset_seconds=alignment_offset_seconds,
            )

        if moment is None:
            moment = get_current_time()

        moment_arrow = arrow.get(moment).to(tzone)
        offset = moment_arrow.utcoffset()
        if offset:
            alignment_offset_seconds = int(offset.total_seconds())
        else:
            alignment_offset_seconds = 0

        floor_dt = cls.calculate_floor(moment_arrow.datetime)
        ceiling_dt = cls.calculate_ceiling(moment_arrow.datetime) or (
            floor_dt + timedelta(hours=1)
        )

        return cls(
            floor=to_utc_aware(floor_dt),
            ceiling=to_utc_aware(ceiling_dt),
            alignment_offset_seconds=alignment_offset_seconds,
        )

    @model_validator(mode="after")
    def _validate_frame(self) -> "TimeFrame":
        if self.floor >= self.ceiling:
            raise ValueError("Floor must be before ceiling")

        # To validate the ceiling, we must perform the calculation in the frame's
        # original timezone context, which is stored in the alignment offset.
        local_floor = to_offset_aware(self.floor, self.alignment_offset_seconds)
        calculated_ceiling_local = self.calculate_ceiling(local_floor)

        if calculated_ceiling_local:
            # Convert the locally calculated ceiling back to UTC for comparison.
            calculated_ceiling_utc = to_utc_aware(calculated_ceiling_local)
            if (
                calculated_ceiling_utc != self.ceiling
                and type(self) is not TimeFrame
            ):
                raise ValueError("Ceiling does not match the calculated frame")

        return self

    @classmethod
    def calculate_floor(cls, moment: datetime) -> datetime:
        return moment  # default to reflect back the moment.

    @classmethod
    def calculate_ceiling(cls, moment: datetime) -> Optional[datetime]:
        return None

    @classmethod
    def get_inner_frame_type(cls) -> Optional[type["TimeFrame"]]:
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
        return self.floor <= current < self.ceiling

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
        return f"{arrow.get(floor).format('YYYY-MM-DD')} - {arrow.get(ceiling).format('YYYY-MM-DD') if ceiling else '__'}"

    def get_next_frame(self) -> "TimeFrame":
        """
        Gets the next time frame.

        Returns:
            TimeFrame: The next time frame.
        """
        tzone = timezone(timedelta(seconds=self.alignment_offset_seconds))
        return self.create(moment=self.ceiling, tzone=tzone)

    def get_previous_frame(self) -> "TimeFrame":
        """
        Gets the previous time frame.

        Returns:
            TimeFrame: The previous time frame.
        """
        tzone = timezone(timedelta(seconds=self.alignment_offset_seconds))
        # Subtract a microsecond to get a moment guaranteed to be in the previous frame.
        return self.create(moment=self.floor - timedelta(microseconds=1), tzone=tzone)

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
        return self.ceiling - self.floor

    def get_floor(self) -> datetime:
        """
        Gets the floor of the time frame, adjusted to its local timezone.

        Returns:
            datetime: The floor of the time frame.
        """
        return to_offset_aware(self.floor, self.alignment_offset_seconds)

    def get_ceiling(self) -> datetime:
        """
        Gets the ceiling of the time frame, adjusted to its local timezone.

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

    @classmethod
    def calculate_floor(cls, moment: datetime) -> datetime:
        return arrow.get(moment).floor("hour").datetime

    @classmethod
    def calculate_ceiling(cls, moment: datetime) -> datetime:
        return arrow.get(moment).floor("hour").shift(hours=1).datetime

    def get_pretty_name(self) -> str:
        return "Hourly"

    def get_pretty_value(self) -> str:
        floor = self.get_floor()
        ceiling = self.get_ceiling()
        return f"{arrow.get(floor).format('MM/DD: hh')} - {arrow.get(ceiling).format('h A') if ceiling else '___'}"


class DailyFrame(TimeFrame):
    """
    A time frame that represents a day.
    """

    @classmethod
    def calculate_floor(cls, moment: datetime) -> datetime:
        return arrow.get(moment).floor("day").datetime

    @classmethod
    def calculate_ceiling(cls, moment: datetime) -> datetime:
        return arrow.get(moment).floor("day").shift(days=1).datetime

    @classmethod
    def get_inner_frame_type(cls) -> type[TimeFrame]:
        return HourlyFrame

    def get_pretty_name(self) -> str:
        return "Daily"

    def get_pretty_value(self) -> str:
        return arrow.get(self.get_floor()).format("MM/DD")


class WeeklyFrame(TimeFrame):
    """
    A time frame that represents a week (starting on Monday).
    """

    @classmethod
    def calculate_floor(cls, moment: datetime) -> datetime:
        return arrow.get(moment).floor("week").datetime

    @classmethod
    def calculate_ceiling(cls, moment: datetime) -> datetime:
        return arrow.get(moment).floor("week").shift(weeks=1).datetime

    @classmethod
    def get_inner_frame_type(cls) -> type[TimeFrame]:
        return DailyFrame

    def get_pretty_name(self) -> str:
        return "Weekly"

    def get_pretty_value(self) -> str:
        floor = self.get_floor()
        ceiling = self.get_ceiling()
        return f"{arrow.get(floor).format('MM/DD')} - {arrow.get(ceiling).format('MM/DD') if ceiling else '__'}"


class MonthlyFrame(TimeFrame):
    """
    A time frame that represents a month.
    """

    @classmethod
    def calculate_floor(cls, moment: datetime) -> datetime:
        return arrow.get(moment).floor("month").datetime

    @classmethod
    def calculate_ceiling(cls, moment: datetime) -> datetime:
        return arrow.get(moment).floor("month").shift(months=1).datetime

    @classmethod
    def get_inner_frame_type(cls) -> type[TimeFrame]:
        return WeeklyFrame

    def get_pretty_name(self) -> str:
        return "Monthly"

    def get_pretty_value(self) -> str:
        return arrow.get(self.get_floor()).format("MMM YYYY")


class QuarterlyFrame(TimeFrame):
    """
    A time frame that represents a quarter.
    """

    @classmethod
    def calculate_floor(cls, moment: datetime) -> datetime:
        return arrow.get(moment).floor("quarter").datetime

    @classmethod
    def calculate_ceiling(cls, moment: datetime) -> datetime:
        return arrow.get(moment).floor("quarter").shift(quarters=1).datetime

    @classmethod
    def get_inner_frame_type(cls) -> type[TimeFrame]:
        return MonthlyFrame

    def get_pretty_name(self) -> str:
        return "Quarterly"

    def get_pretty_value(self) -> str:
        return arrow.get(self.get_floor()).format("[Q]Q YYYY")


class YearlyFrame(TimeFrame):
    """
    A time frame that represents a year.
    """

    @classmethod
    def calculate_floor(cls, moment: datetime) -> datetime:
        return arrow.get(moment).floor("year").datetime

    @classmethod
    def calculate_ceiling(cls, moment: datetime) -> datetime:
        return arrow.get(moment).floor("year").shift(years=1).datetime

    @classmethod
    def get_inner_frame_type(cls) -> type[TimeFrame]:
        return QuarterlyFrame

    def get_pretty_name(self) -> str:
        return "Yearly"

    def get_pretty_value(self) -> str:
        return arrow.get(self.get_floor()).format("YYYY")


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
    # Use a moment in the middle of the frame for robustness against DST changes.
    mid_point_utc = floor + (ceiling - floor) / 2
    tzone = timezone(timedelta(seconds=alignment_offset_seconds))
    moment_local = to_offset_aware(mid_point_utc, alignment_offset_seconds)

    align_type: Optional[type[TimeFrame]] = YearlyFrame
    while align_type:
        # Construct a potential frame using the midpoint.
        potential = align_type.create(moment=moment_local, tzone=tzone)

        # Check if this potential frame exactly matches the provided boundaries.
        if (
            potential.floor == to_utc_aware(floor)
            and potential.ceiling == to_utc_aware(ceiling)
            and potential.alignment_offset_seconds == alignment_offset_seconds
        ):
            return potential
        align_type = align_type.get_inner_frame_type()

    return TimeFrame.create(
        floor=floor,
        ceiling=ceiling,
        alignment_offset_seconds=alignment_offset_seconds,
    )


# -- Module-level Mappings --
_RESOLUTION_TO_FRAME_CLASS_MAP: dict[TimeFrameResolution, type[TimeFrame]] = {
    TimeFrameResolution.hourly: HourlyFrame,
    TimeFrameResolution.daily: DailyFrame,
    TimeFrameResolution.weekly: WeeklyFrame,
    TimeFrameResolution.monthly: MonthlyFrame,
    TimeFrameResolution.quarterly: QuarterlyFrame,
    TimeFrameResolution.yearly: YearlyFrame,
}
_FRAME_CLASS_TO_RESOLUTION_MAP: dict[type[TimeFrame], TimeFrameResolution] = {
    v: k for k, v in _RESOLUTION_TO_FRAME_CLASS_MAP.items()
}