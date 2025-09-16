from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, List, Optional, Tuple

from pymongo import ASCENDING, DESCENDING

class LoadDirective(ABC):
    @abstractmethod
    def get_exp(self) -> Any:
        pass


class MatchDirective(ABC):
    """
    An abstract base class for creating match directives.

    Match directives are used to create complex queries in a more readable
    way.
    """

    @abstractmethod
    def to_filter(self) -> dict:
        """
        Converts the match directive to a MongoDB filter.

        Returns:
            dict: A MongoDB filter.
        """
        pass

class Size(LoadDirective):
    def __init__(self, value: int) -> None:
        self._value = value

    def get_exp(self):
        return self._value


class SortOp(LoadDirective):
    def __init__(self, value: Optional[dict] = None) -> None:
        self._value = value

    def __and__(self, sort: "SortOp") -> "SortOp":
        value1 = self.get_exp()
        value2 = sort.get_exp()
        if value1 is None:
            return sort

        if value2 is None:
            return self

        return SortOp(value1 | value2)

    def has_sort_op(self):
        return self._value is not None and len(self._value) > 0

    def get_tuples(self) -> Optional[List[Tuple[str, int]]]:
        sort_values = self.get_exp()
        if sort_values is None:
            return None

        return [(key, value) for key, value in sort_values.items()]

    def get_exp(self):
        return self._value


class SortAsc(SortOp):
    def __init__(self, field: str) -> None:
        self._value = {field: ASCENDING}


class SortDesc(SortOp):
    def __init__(self, field: str) -> None:
        self._value = {field: DESCENDING}

class Filter(LoadDirective):
    def __init__(self, query: Optional[dict] = None) -> None:
        self._value = query if query is not None else {}

    def __and__(self, filter: "Filter") -> "Filter":
        retval = Filter()
        retval._value = self.get_exp() | filter.get_exp()
        return retval

    def has_filter(self):
        return self._value is not None and len(self._value) > 0

    def get_exp(self):
        return convert_to_match_expression(self._value)
    
    @classmethod
    def fields(cls, **kwargs) -> "Filter":
        return cls(query=kwargs)
    

def convert_to_match_expression(kwargs: dict) -> dict:
    """
    Converts a dictionary of keyword arguments to a MongoDB match filter.

    Args:
        kwargs (dict): The keyword arguments to convert.

    Returns:
        dict: A MongoDB match filter.
    """
    return {
        key: parse_match_directive(value)
        for key, value in kwargs.items()
        if parse_match_directive(value) is not None
    }


def parse_match_directive(value):
    """
    Parses a match directive.

    Args:
        value: The value to parse.

    Returns:
        The parsed value.
    """
    if isinstance(value, MatchDirective):
        return value.to_filter()

    if isinstance(value, Enum):
        if value.value == "ANY":
            return None
        return value.value

    if isinstance(value, LoadDirective):
        raise ValueError(
            f"Detected a loading directive in match filter {type(value).__name__}.  Possible mismatch argument (check spelling)"
        )

    return value