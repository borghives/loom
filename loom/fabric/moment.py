from typing import Optional
from pydantic import Field
from datetime import datetime
from functools import total_ordering
from pydantic import ConfigDict
from loom.info.model import StrUpper
from loom.info.persist import Persistable
from loom.time.util import to_utc_aware

"""
A Moment represents a snapshot in time where all related market metrics are consistent with each other.
Instead of relying on a continuous flow of time, our system operates on a discrete sequence of these Moments.
"""


@total_ordering
class Moment(Persistable):
    """
    A concept representing a moment of time.

    A Moment is a point in time for a specific symbol, containing related data. Moments are comparable based on their time.
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
    def create(cls, time: Optional[datetime] = None, **kwargs):
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
        retval = cls(**kwargs, updated_time=time)
        return retval
