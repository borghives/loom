from typing import Optional
from pydantic import Field
from datetime import datetime
from functools import total_ordering
from pydantic import ConfigDict
from loom.info.model import StrUpper
from loom.info.persist import Persistable
from loom.time.util import to_utc_aware

@total_ordering
class Moment(Persistable):
    """A concept representing a moment of time"""

    symbol: StrUpper = Field(description="The dimension of the event keyed by symbol")
    
    #allow extra fields for flexibility in inheriting data moments
    model_config = ConfigDict(extra='allow')

    def __eq__(self, other):
        if not isinstance(other, Moment):
            return NotImplemented
        return self.time == other.time

    def __lt__(self, other):
        if not isinstance(other, Moment):
            return NotImplemented
        return self.time < other.time
    
    @property
    def time(self) -> datetime:
        if self.updated_time is None:
            raise ValueError("No associated time yet.  Moment should always have a associated time.")
        return to_utc_aware(self.updated_time)
    
    @property
    def date_str(self) -> str:
        return self.time.strftime("%Y-%m-%d")
    
    @classmethod
    def create(cls, event_time: Optional[datetime] = None, **kwargs):
        retval = cls(**kwargs, updated_time=event_time)
        return retval