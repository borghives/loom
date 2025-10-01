import time
import functools
from typing import Callable, Optional, Any
from contextlib import nullcontext

class PerfTimer:
    def __init__(self, name: Optional[str] = None, depth: int = 0, verbose: bool = True):
        self.name = name
        self.verbose = verbose
        self.child_timers: dict[str, PerfTimer] = {}
        self.total_time: float = 0.0
        self.count: int = 0
        self.depth: int = depth
        self._start_time: Optional[float] = None
    
    def start(self):
        self._start_time = time.perf_counter()

    def stop(self):
        if self._start_time is not None:
            self.total_time += (time.perf_counter() - self._start_time)
            self._start_time = None
            self.count += 1

            for child in self.child_timers.values():
                child.stop()

            if self.verbose:
                print(self)

    def sub_timer(self, name: str, verbose: Optional[bool] = None) -> 'PerfTimer':
        if name not in self.child_timers:
            self.child_timers[name] = PerfTimer(name=name, depth=self.depth + 1, verbose=verbose if verbose is not None else self.verbose)
        return self.child_timers[name]

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

    @property
    def elapsed(self) -> float:
        """Elapsed time in seconds since the timer started."""
        if self._start_time is None:
            return 0.0

        return time.perf_counter() - self._start_time

    def __str__(self):
        name_str = f"{self.name}" if self.name else "PerfTimer"
        retval = ""
        indent = "  " * self.depth
        

        if self.total_time < 1e-3:
            duration_str = f"{self.total_time * 1e6:.2f} us"
            if (self.count > 1):
                duration_str += f" ({(self.total_time / self.count) * 1e6:.2f} us avg)"
        elif self.total_time < 1:
            duration_str = f"{self.total_time * 1e3:.2f} ms"
            if (self.count > 1):
                duration_str += f" ({(self.total_time / self.count) * 1e3:.2f} ms avg)"
        else:
            duration_str = f"{self.total_time:.4f} s"
            if (self.count > 1):
                duration_str += f" ({(self.total_time / self.count):.4f} s avg)"

        retval = f"\n{indent} - {name_str} -> {self.count} times in {duration_str}"

        if len(self.child_timers):
            for child in self.child_timers.values():
                retval += f"{child}"
        
        return retval

def sub_timed(instance: Optional[PerfTimer | nullcontext], name: str, verbose: Optional[bool] = None) -> PerfTimer | nullcontext:
    """
    Create a sub-timer context from an existing PerfTimer instance.
    
    If the instance is None, returns a nullcontext.
    """
    if instance is None or isinstance(instance, nullcontext):
        return nullcontext()
    return instance.sub_timer(name=name, verbose=verbose)

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