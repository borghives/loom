from datetime import datetime
from typing import Optional

from loom.fabric.moment import Moment
from pyrsistent import pvector, PVector

from loom.time.timeframing import DailyFrame
from loom.time.util import EASTERN_TIMEZONE, to_utc_aware

class MomentWindow:
    """A window of moments in ascending time order"""
    moments : PVector[Moment]
    _symbol : Optional[str] = None

    @property
    def symbol(self) -> str:
        return self._symbol.upper() if self._symbol else ""

    def __init__(self, moments : Optional[list[Moment] | PVector[Moment]] = None, symbol : Optional[str] = None):
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
        if len(self) == 0:
            return "[empty]"
        return f"{self._symbol} : {self.moments[0].date_str} - {self.moments[-1].date_str}"

    def __len__(self):
        return len(self.moments)
    
    def __iter__(self):
        return iter(self.moments)
    
    def __getitem__(self, key):
        if isinstance(key, slice):
            return MomentWindow(self._symbol, self.moments[key])
        elif isinstance(key, int):
            return self.moments[key]
        else:
            raise TypeError(
                f"{self.__class__.__name__} indices must be integers or slices, "
                f"not {type(key).__name__}"
            )
        
    def sliding_window(self, window_size: int):
        for i in range(len(self) - window_size + 1):
            yield MomentWindow(moments=self.moments[i:i+window_size], symbol=self.symbol)

    def sliding_time_cone(self, past_size: int, future_size: int):
        for i in range(len(self) - past_size + 1):
            yield MomentWindow(moments=self.moments[i:i+past_size], symbol=self.symbol), MomentWindow(
                    moments=self.moments[i+past_size:min(i+past_size+future_size, len(self))], 
                    symbol=self.symbol
                )

    def get_moments(self, after : Optional[datetime] = None, before : Optional[datetime] = None) :
        return [moment for moment in self.moments 
                if moment.time and (after is None or moment.time >= to_utc_aware(after)) 
                and (before is None or moment.time <= to_utc_aware(before))
        ]
    
    def get_day_frame(self):
        latest = self.get_latest()
        frame = DailyFrame(moment=latest.time, tzone=EASTERN_TIMEZONE)
        return frame
    
    def get_latest(self) :
        return self.get_moments()[-1]