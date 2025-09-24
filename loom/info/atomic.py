
from typing import Annotated

from pydantic import AfterValidator

from loom.info.field import BeforeSetAttr, CoalesceOnIncr, InitializeValue


class IntCounter(int):
    """
    An integer that can be incremented atomically.

    This class is useful for creating fields that can be incremented in the
    database

    Attributes:
        _incr_value (int): The amount to increment the value by.
    """

    _incr_value: int = 0

    def get_changes(self) -> int:
        """
        Gets the amount to increment the value by.

        Returns:
            int: The amount to increment the value by.
        """
        return self._incr_value

    def __iadd__(self, incr_value: int) -> "IntCounter":
        """
        Increments the value by the given amount.

        Args:
            incr_value (int): The amount to increment the value by.

        Returns:
            IntCounter: The new value.
        """
        value = int(self) + incr_value
        retval = IntCounter(value)
        retval._incr_value = self._incr_value + incr_value
        return retval

    def collapse(self) -> "IntCounter":
        """
        Returned a new IntCounter with the increment.
        """
        return IntCounter(int(self) + self._incr_value)

def check_int_counter(value):
    if isinstance(value, IntCounter) :
        return value
    raise AttributeError("Cannot set on an IncrIntCounter directly. Must use += operator to increment.")

def validate_int_counter(value):
    if value is None:
        return IntCounter(0)
    if isinstance(value, IntCounter):
        return value
    return IntCounter(value)

IncrCounter = Annotated[int, 
    CoalesceOnIncr(collapse=lambda x: x.collapse()), 
    BeforeSetAttr(check_int_counter),
    InitializeValue(validate_int_counter),
    AfterValidator(validate_int_counter)]