import time
import functools
from typing import Callable, Optional, Any
from contextlib import nullcontext

class PerfTimer:
    def __init__(self, name: Optional[str] = None, depth: int = 0, verbose: bool = False):
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
    if instance is None or not isinstance(instance, PerfTimer):
        return nullcontext()
    return instance.sub_timer(name=name, verbose=verbose)

def timed(func: Optional[Callable] = None, *, name: Optional[str] = None, verbose: bool = False) -> Callable:
    """
    A decorator to time a function.
    
    Can be used as `@timed` or with arguments: `@timed(name='My Function')`.

    This decorator also supports nested timing. If the decorated function is
    called with a keyword argument `ptimer` that is a `PerfTimer` instance,
    the function will be timed as a sub-timer within that parent timer.

    Args:
        func: The function to decorate.
        name: An optional name for the timer. Defaults to the function name.
        verbose: Whether to print the timing results.

    Keyword Args (for decorated function):
        ptimer (PerfTimer, optional): An existing PerfTimer instance to attach
            this timing to as a sub-timer.
    """
    if func is None:
        return functools.partial(timed, name=name, verbose=verbose)

    @functools.wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        timer_name = name or func.__name__
        ptimer = kwargs.get('ptimer')
        if ptimer is not None and isinstance(ptimer, PerfTimer):
            perf_timer = sub_timed(ptimer, name=timer_name, verbose=verbose)
        else:
            perf_timer = PerfTimer(name=timer_name, verbose=verbose)

        with perf_timer:
            if ('ptimer' in func.__annotations__):
                kwargs['ptimer'] = perf_timer
            return func(*args, **kwargs)
    return wrapper