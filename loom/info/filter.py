
from typing import Optional, Union

from loom.info.expression import Expression
from loom.info.query_op import And, Or, QueryOpExpression

class Filter(Expression):
    """Filter is MongoDb Query Predicate Expression"""
    def __init__(self, query: Optional[Union[dict, QueryOpExpression]] = None) -> None:
        self._value = query if query is not None else {}

    @property
    def repr_value(self):
        return self._value
    
    @classmethod
    def wrap(cls, value) -> "Filter":
        if isinstance(value, Filter):
            return value
        
        if isinstance(value, dict):
            return cls(value)
        
        raise ValueError(f"unsupported type {type(value)} to wrap filter")

    def __and__(self, other):
        """
        Combines this filter with another using a logical AND.
        """
        if other is None:
            return self

        if not isinstance(other, Filter):
            other = Filter.wrap(other)

        if self.is_empty():
            return other
        if other.is_empty():
            return self

        self_clauses = self._value.data if isinstance(self._value, And) else [self]
        other_clauses = other._value.data if isinstance(other._value, And) else [other]
            
        return Filter(And(self_clauses + other_clauses))


    def __or__(self, other):
        """
        Combines this filter with another using a logical OR.
        """
        if other is None:
            return self

        if not isinstance(other, Filter):
            other = Filter.wrap(other)

        if self.is_empty():
            return other
        
        if other.is_empty():
            return self

        self_clauses = self._value.data if isinstance(self._value, Or) else [self]
        other_clauses = other._value.data if isinstance(other._value, Or) else [other]
                
        return Filter(Or(self_clauses + other_clauses))