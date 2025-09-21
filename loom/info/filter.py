
from typing import Optional
from loom.info.expression import Expression


class Filter(Expression):
    """Filter is MongoDb Query Predicate Expression"""
    def __init__(self, query: Optional[dict] = None) -> None:
        self._value = query if query is not None else {}

    def express(self):
        """Convert query operator expression"""

        return {
            key: marshal_expression(value)
            for key, value in self._value.items()
        }
    
    @classmethod
    def wrap(cls, value) -> "Filter":
        if isinstance(value, Filter):
            return value
        
        if isinstance(value, dict):
            return cls(value)
        
        raise ValueError(f"unsupported type {type(value)} to wrap filter")

def marshal_expression(value):
    """
    Parses a match directive.

    Args:
        value: The value to parse.

    Returns:
        The parsed value.
    """
    if isinstance(value, Expression):
        return value.express()

    return value