from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Optional
from zoneinfo import ZoneInfo

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
    def create(cls, moment: Optional[datetime] = None, tzone: timezone | ZoneInfo = timezone.utc) -> "TimeFrame":
        """
        Create a TimeFrame instance.

        Args:
            moment (Optional[datetime], optional): A moment in time to align
                the frame to. Defaults to `None`.
            **kwargs: Additional keyword arguments to pass to the constructor.

        Returns:
            TimeFrame: The created TimeFrame instance.
        """
        if moment is None:
            moment = get_current_time()

        moment = to_utc_aware(moment)

        if tzone:
            moment = moment.astimezone(tzone)

        alignment_offset_seconds = 0
        moment_utc_offset = moment.utcoffset()
        if moment_utc_offset:
            alignment_offset_seconds = int(moment_utc_offset.total_seconds())

        floor = cls.calculate_floor(moment)
        ceiling = cls.calculate_ceiling(moment) or (floor + timedelta(hours=1))

        return cls(floor=floor, ceiling=ceiling, alignment_offset_seconds=alignment_offset_seconds)  # type: ignore
    
    @model_validator(mode="before")
    @classmethod
    def _construct_and_prepare_fields(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data

        floor = data.get("floor")
        ceiling = data.get("ceiling")

        if floor is not None and ceiling is not None:
            # Path 1: Direct construction from floor and ceiling
            data["floor"] = to_utc_aware(floor)
            data["ceiling"] = to_utc_aware(ceiling)
        elif floor is None and ceiling is None:
            # Path 2: Construct from a moment in time
            moment = data.get("moment")
            tzone = data.get("tzone")

            if moment is None:
                moment = get_current_time()

            moment = to_utc_aware(moment)

            if tzone:
                moment = moment.astimezone(tzone)

            alignment_offset_seconds = 0
            moment_utc_offset = moment.utcoffset()
            if moment_utc_offset:
                alignment_offset_seconds = int(moment_utc_offset.total_seconds())

            floor = cls.calculate_floor(moment)
            ceiling = cls.calculate_ceiling(moment) or (floor + timedelta(hours=1))

            data.update(
                {
                    "floor": to_utc_aware(floor),
                    "ceiling": to_utc_aware(ceiling),
                    "alignment_offset_seconds": alignment_offset_seconds,
                }
            )
        else:
            raise ValueError(
                "Either both floor and ceiling must be provided, or neither."
            )

        return data

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
        return f"{floor.strftime('%Y-%m-%d')} - {ceiling.strftime('%Y-%m-%d') if ceiling else '__'}"

    def get_next_frame(self) -> "TimeFrame":
        """
        Gets the next time frame.

        Returns:
            TimeFrame: The next time frame.
        """
        return self.create(moment=self.get_ceiling())

    def get_previous_frame(self) -> "TimeFrame":
        """
        Gets the previous time frame.

        Returns:
            TimeFrame: The previous time frame.
        """
        # Subtract a microsecond to get a moment guaranteed to be in the previous frame.
        return self.create(moment=self.get_floor() - timedelta(microseconds=1))

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
        return moment.replace(minute=0, second=0, microsecond=0)

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

    @classmethod
    def calculate_floor(cls, moment: datetime) -> datetime:
        return moment.replace(hour=0, minute=0, second=0, microsecond=0)

    @classmethod
    def calculate_ceiling(cls, moment: datetime) -> datetime:
        return cls.calculate_floor(moment) + timedelta(days=1)

    @classmethod
    def get_inner_frame_type(cls) -> type[TimeFrame]:
        return HourlyFrame

    def get_pretty_name(self) -> str:
        return "Daily"

    def get_pretty_value(self) -> str:
        return f"{self.get_floor().strftime('%m/%d')}"


class WeeklyFrame(TimeFrame):
    """
    A time frame that represents a week (starting on Monday).
    """

    @classmethod
    def calculate_floor(cls, moment: datetime) -> datetime:
        floor = moment - timedelta(days=moment.weekday())
        return floor.replace(hour=0, minute=0, second=0, microsecond=0)

    @classmethod
    def calculate_ceiling(cls, moment: datetime) -> datetime:
        return cls.calculate_floor(moment) + timedelta(days=7)

    @classmethod
    def get_inner_frame_type(cls) -> type[TimeFrame]:
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

    @classmethod
    def calculate_floor(cls, moment: datetime) -> datetime:
        return moment.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    @classmethod
    def calculate_ceiling(cls, moment: datetime) -> datetime:
        # To get the start of the next month, we add a duration longer
        # than any possible month (32 days) to the start of the current
        # month, and then simply snap to the 1st day of that resulting month.
        one_more_day_than_longest_month = timedelta(days=32)
        some_day_next_month = (
            cls.calculate_floor(moment) + one_more_day_than_longest_month
        )
        return some_day_next_month.replace(day=1)

    @classmethod
    def get_inner_frame_type(cls) -> type[TimeFrame]:
        return WeeklyFrame

    def get_pretty_name(self) -> str:
        return "Monthly"

    def get_pretty_value(self) -> str:
        return f"{self.get_floor().strftime('%b %Y')}"


class QuarterlyFrame(TimeFrame):
    """
    A time frame that represents a quarter.
    """

    @classmethod
    def calculate_floor(cls, moment: datetime) -> datetime:
        quarter = (moment.month - 1) // 3
        return moment.replace(
            month=quarter * 3 + 1, day=1, hour=0, minute=0, second=0, microsecond=0
        )

    @classmethod
    def calculate_ceiling(cls, moment: datetime) -> datetime:
        # To get the start of the next quarter, we add a duration longer
        # than any possible quarter (93 days) to the start of the current
        # quarter, and then snap to the 1st day of the resulting month,
        # which will be the first month of the next quarter.
        one_more_day_than_longest_quarter = timedelta(days=93)
        some_day_next_quarter = (
            cls.calculate_floor(moment) + one_more_day_than_longest_quarter
        )
        # This works because adding ~3 months to the first month of a quarter
        # (Jan, Apr, Jul, Oct) will always land in the first month of the next quarter.
        return some_day_next_quarter.replace(
            day=1, hour=0, minute=0, second=0, microsecond=0
        )

    @classmethod
    def get_inner_frame_type(cls) -> type[TimeFrame]:
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

    @classmethod
    def calculate_floor(cls, moment: datetime) -> datetime:
        return moment.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)

    @classmethod
    def calculate_ceiling(cls, moment: datetime) -> datetime:
        # Add 366 days to safely land in the next year, then snap to the
        # beginning of that year.
        return (cls.calculate_floor(moment) + timedelta(days=366)).replace(
            month=1, day=1
        )

    @classmethod
    def get_inner_frame_type(cls) -> type[TimeFrame]:
        return QuarterlyFrame

    def get_pretty_name(self) -> str:
        return "Yearly"

    def get_pretty_value(self) -> str:
        return f"{self.get_floor().year}"


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
    moment_local = to_offset_aware(mid_point_utc, alignment_offset_seconds)
    tzone = timezone(timedelta(seconds=alignment_offset_seconds))

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

    return TimeFrame(
        floor=floor, ceiling=ceiling, alignment_offset_seconds=alignment_offset_seconds
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