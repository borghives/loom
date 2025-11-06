from typing import List, Optional, Tuple
from pymongo import ASCENDING, DESCENDING
from loom.info.expression import Expression, ExpressionDriver
from loom.info.predicate import FieldName

class SortOp(Expression):
    """
    An expression representing a MongoDB sort operation.

    Sort operations can be combined using the `&` operator.
    """
    def __init__(self, value: Optional[dict] = None) -> None:
        self._value = value

    @property
    def repr_value(self):
        return self._value
    
    @classmethod
    def wrap(cls, value) -> "SortOp":
        if isinstance(value, SortOp):
            return value

        if isinstance(value, dict):
            return cls(value)
        
        raise ValueError(f"unsupported type {type(value)} to wrap sort")

    def get_tuples(self, driver: Optional[ExpressionDriver] = None) -> Optional[List[Tuple[str, int]]]:
        sort_values = self.express(driver)
        if sort_values is None:
            return None
        
        assert isinstance(sort_values, dict)
        return [(str(key), int(value)) for key, value in sort_values.items()]
    
    def __and__(self, other):
        if other is None:
            return self
        
        if not isinstance(other, SortOp):
            other = SortOp.wrap(other)

        if self.is_empty():
            return other
        
        if other.is_empty():
            return self
        
        self_value = self.repr_value
        other_value = other.repr_value

        assert isinstance(self_value, dict)
        assert isinstance(other_value, dict)
        
        combined = self_value | other_value

        return SortOp(combined)
        
class SortAsc(SortOp):
    def __init__(self, field: str) -> None:
        self._value = {FieldName(field): ASCENDING}

class SortDesc(SortOp):
    def __init__(self, field: str) -> None:
        self._value = {FieldName(field): DESCENDING}