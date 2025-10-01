import time
import functools
from typing import Callable, Optional, Any

class PerfTimer:
    def __init__(self, name: Optional[str] = None, verbose: bool = True):
        self.name = name
        self.verbose = verbose
        self._start_time: Optional[float] = None
        self._end_time: Optional[float] = None

    def __enter__(self):
        self._start_time = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._end_time = time.perf_counter()
        if self.verbose:
            print(self)

    @property
    def duration(self) -> float:
        """Duration in seconds, or None if the timer hasn't finished."""
        if self._start_time is None or self._end_time is None:
            return 0.0
        return self._end_time - self._start_time

    @property
    def elapsed(self) -> float:
        """Elapsed time in seconds since the timer started."""
        if self._start_time is None:
            return 0.0
        if self._end_time is None:
            return time.perf_counter() - self._start_time
        return self.duration or 0.0

    def __str__(self):
        name_str = f"{self.name}" if self.name else "PerfTimer"
        duration = self.duration
        if duration is None:
            duration = self.elapsed

        if duration < 1e-3:
            return f"{name_str} elapsed: {duration * 1e6:.2f} us"
        if duration < 1:
            return f"{name_str} elapsed: {duration * 1e3:.2f} ms"
        return f"{name_str} elapsed: {duration:.4f} s"


def timed(func: Optional[Callable] = None, *, name: Optional[str] = None, verbose: bool = True) -> Callable:
    """
    A decorator to time a function.
    
    Can be used as `@timed` or with arguments: `@timed(name='My Function')`.
    """
    if func is None:
        return functools.partial(timed, name=name, verbose=verbose)

    @functools.wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        timer_name = name or func.__name__
        with PerfTimer(name=timer_name, verbose=verbose):
            return func(*args, **kwargs)
    return wrapper