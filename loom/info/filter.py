
from typing import Optional, Union

from loom.info.expression import Expression
from loom.info.query_op import And, Or, QueryOpExpression

class QueryPredicates(Expression):
    """
    An expression that represents a MongoDB query predicate (the part of a `find`
    operation that selects documents).

    Filters can be combined using logical `&` (and) and `|` (or) operators.

    ref: https://www.mongodb.com/docs/manual/reference/mql/query-predicates/
    """
    def __init__(self, query_predicate: Optional[Union[dict, QueryOpExpression]] = None) -> None:
        assert query_predicate is None or isinstance(query_predicate, (dict, QueryOpExpression))
        self._value = query_predicate if query_predicate is not None else {}

    @property
    def repr_value(self):
        return self._value

    def __and__(self, other):
        """
        Combines this filter with another using a logical AND.
        """
        if other is None:
            return self

        if not isinstance(other, QueryPredicates):
            other = QueryPredicates(other)

        if self.is_empty():
            return other
        if other.is_empty():
            return self

        self_clauses = self.repr_value.data if isinstance(self.repr_value, And) else [self]
        other_clauses = other.repr_value.data if isinstance(other.repr_value, And) else [other]
            
        return QueryPredicates(And(self_clauses + other_clauses))


    def __or__(self, other):
        """
        Combines this filter with another using a logical OR.
        """
        if other is None:
            return self

        if not isinstance(other, QueryPredicates):
            other = QueryPredicates(other)

        if self.is_empty():
            return other
        
        if other.is_empty():
            return self

        self_clauses = self.repr_value.data if isinstance(self.repr_value, Or) else [self]
        other_clauses = other.repr_value.data if isinstance(other.repr_value, Or) else [other]
                
        return QueryPredicates(Or(self_clauses + other_clauses))