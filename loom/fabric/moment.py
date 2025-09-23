from typing import Iterator, Optional, Union
from pydantic import Field
from datetime import datetime
from functools import total_ordering
from pydantic import ConfigDict
from pyrsistent import PVector, pvector
from loom.info.model import StrUpper
from loom.info.persistable import Persistable
from loom.time.timeframing import DailyFrame, TimeFrame
from loom.time.util import EASTERN_TIMEZONE, to_utc_aware

"""
A Moment represents a snapshot in time where all related market metrics are consistent with each other.
Instead of relying on a continuous flow of time, our system operates on a discrete sequence of these Moments.
"""


@total_ordering
class Moment(Persistable):
    """
    A concept representing a moment of time.

    A Moment is a point in time for a specific symbol dimension, containing related data. 
    Moments are comparable based on their time.
    """

    symbol: StrUpper = Field(
        description="The dimension of the event moment keyed by symbol"
    )

    # allow extra fields for flexibility in inheriting data moments
    model_config = ConfigDict(extra="allow")

    def __eq__(self, other):
        """
        Check for equality with another Moment object based on their time attribute.

        Args:
            other: The object to compare with.

        Returns:
            True if the objects are equal, otherwise NotImplemented.
        """
        if not isinstance(other, Moment):
            return NotImplemented
        return self.time == other.time

    def __lt__(self, other):
        """
        Compare if this Moment is less than another Moment based on their time attribute.

        Args:
            other: The object to compare with.

        Returns:
            True if this Moment's time is less than the other's, otherwise NotImplemented.
        """
        if not isinstance(other, Moment):
            return NotImplemented
        return self.time < other.time

    @property
    def time(self) -> datetime:
        """
        The UTC-aware datetime of the moment.

        Raises:
            ValueError: If `updated_time` is not set.

        Returns:
            The UTC-aware datetime of the moment.
        """
        if self.updated_time is None:
            raise ValueError(
                "No associated time yet.  Moment should always have a associated time."
            )
        return to_utc_aware(self.updated_time)

    @property
    def date_str(self) -> str:
        """
        The date part of the moment's time as a 'YYYY-MM-DD' formatted string.

        Returns:
            The date string.
        """
        return self.time.strftime("%Y-%m-%d")

    @classmethod
    def create(cls, time: datetime, **kwargs):
        """
        Create a Moment instance.

        This classmethod allows for the creation of a Moment instance, setting the `updated_time`
        from the `time` parameter.

        Args:
            time: The datetime for the moment.
            **kwargs: Additional keyword arguments for the Moment.

        Returns:
            A new Moment instance.
        """
        retval = cls(updated_time=time, **kwargs)
        return retval


class MomentWindow[T : Moment]:
    """
    A window of moments in ascending time order.

    This class acts as a container for a sequence of `Moment` objects, providing
    utilities for slicing, iterating, and filtering them.
    """
    moments: PVector[T]
    _symbol: Optional[str] = None

    @property
    def symbol(self) -> str:
        """
        The symbol associated with the moments in this window.

        Returns:
            The symbol as an uppercase string, or an empty string if not set.
        """
        return self._symbol.upper() if self._symbol else ""

    def __init__(self, moments: Optional[list[T] | PVector[T]] = None, symbol: Optional[str] = None):
        """
        Initialize a MomentWindow.

        Args:
            moments: An optional list or pvector of `Moment` objects. If provided,
                they will be sorted and filtered.
            symbol: An optional symbol to associate with the window. If not provided,
                it will be inferred from the first moment.
        """
        if moments is None:
            self.moments = pvector()
            self._symbol = symbol if symbol else None
        else:
            self._symbol = symbol if symbol else moments[0].symbol if len(moments) > 0 else None
            if isinstance(moments, PVector):
                self.moments = pvector(moments)

            else:
                moments = [moment for moment in moments if moment.is_entangled() and moment.symbol == self._symbol]
                self.moments = pvector(sorted(moments))

    def __str__(self) -> str:
        """
        Return a string representation of the MomentWindow.

        Returns:
            A string showing the symbol and date range, or "[empty]" if the window is empty.
        """
        if len(self) == 0:
            return "[empty]"
        return f"{self._symbol} : {self.moments[0].date_str} - {self.moments[-1].date_str}"

    def __len__(self) -> int:
        """
        Return the number of moments in the window.

        Returns:
            The number of moments.
        """
        return len(self.moments)

    def __iter__(self) -> Iterator[T]:
        """
        Return an iterator over the moments in the window.

        Returns:
            An iterator of `Moment` objects.
        """
        return iter(self.moments)

    def __getitem__(self, key: Union[int, slice]) -> Union[T, 'MomentWindow[T]']:
        """
        Get an item or slice from the window.

        Args:
            key: An integer index or a slice.

        Returns:
            A `Moment` object if the key is an integer, or a new `MomentWindow`
            if the key is a slice.
        
        Raises:
            TypeError: If the key is not an integer or slice.
        """
        if isinstance(key, slice):
            return MomentWindow[T](moments=self.moments[key], symbol=self._symbol)
        elif isinstance(key, int):
            return self.moments[key]
        else:
            raise TypeError(
                f"{self.__class__.__name__} indices must be integers or slices, "
                f"not {type(key).__name__}"
            )

    def sliding_window(self, window_size: int) -> Iterator["MomentWindow[T]"]:
        """
        Generate sliding windows of a specified size.

        Args:
            window_size: The size of each sliding window.

        Yields:

            A `MomentWindow` for each sliding window.
        """
        for i in range(len(self) - window_size + 1):
            yield MomentWindow[T](moments=self.moments[i:i + window_size], symbol=self.symbol)

    def sliding_time_cone(self, past_size: int, future_size: int) -> Iterator[tuple['MomentWindow[T]', 'MomentWindow[T]']]:
        """
        Generate sliding time cones of past and future moments.

        Args:
            past_size: The size of the past window.
            future_size: The size of the future window.

        Yields:
            A tuple containing two `MomentWindow` objects: one for the past and one for the future.
        """
        for i in range(len(self) - past_size + 1):
            yield MomentWindow[T](moments=self.moments[i:i + past_size], symbol=self.symbol), MomentWindow[T](
                moments=self.moments[i + past_size:min(i + past_size + future_size, len(self))],
                symbol=self.symbol
            )

    def get_moments(self, after: Optional[datetime] = None, before: Optional[datetime] = None) -> list[T]:
        """
        Get moments within a specified time range.

        Args:
            after: The start of the time range (inclusive).
            before: The end of the time range (inclusive).

        Returns:
            A list of `Moment` objects within the specified range.
        """
        return [moment for moment in self.moments
                if moment.time and (after is None or moment.time >= to_utc_aware(after))
                and (before is None or moment.time <= to_utc_aware(before))
                ]

    def get_day_frame(self) -> TimeFrame:
        """
        Get the daily frame for the latest moment in the window.

        Returns:
            A `DailyFrame` object.
        """
        latest = self.get_latest()
        frame = DailyFrame.create(moment=latest.time, tzone=EASTERN_TIMEZONE)
        return frame

    def get_latest(self):
        """
        Get the latest moment in the window.

        Returns:
            The latest `Moment` object.
        """
        return self.get_moments()[-1]