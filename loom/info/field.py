
from enum import Enum
from functools import wraps
from typing import Optional

from loom.info.expression import Expression, LiteralInput, FieldName
from loom.info.filter import QueryPredicates
from loom.info.query_op import (
    All,
    Exists,
    In,
    Gte,
    Lte,
    Gt, 
    Lt,
    Ne,
    Eq,
    NotAll,
    NotIn,
    TimeQuery,
    QueryOpExpression,
)
from loom.time.timeframing import TimeFrame

def suppress_warning(func):
    """
    A decorator to acknowledge and document the intentional override of a method
    that may be flagged by static analysis tools like Pylance. This is used
    for methods like `__eq__` and `__ne__` in `QueryableField`, where we are
    intentionally changing the return type to build a query DSL, which deviates
    from the typical signature of these methods.
    """
    @wraps(func) 
    def wrapper(self, other):
        return func(self, other)
    return wrapper

class QueryableField:
    """
    Provides a fluent interface for creating query filters for a specific model field.

    Instances of this class are typically created and used through a `Persistable`
    model's `q` attribute. It uses operator overloading (e.g., `__eq__`, `__gt__`)
    to build `Filter` objects in a highly readable way.

    Example:
        # Assuming `MyModel` is a `Persistable` model
        filter = MyModel.q.age > 30
    """
    def __init__(self, name: str):
        self.name = name

    def get_query_name(self):
        return FieldName(self.name)

    def normalize_literal_input(self, literal_input):
        if isinstance(literal_input, dict):
            return {k: LiteralInput(v).for_fld(self.name) for k, v in literal_input.items()}
    
        if isinstance(literal_input, list):
            return [LiteralInput(v).for_fld(self.name) for v in literal_input]
        
        return LiteralInput(literal_input).for_fld(self.name)

    def __gt__(self, literal_input) -> QueryPredicates:
        if literal_input is None:
            return QueryPredicates()
        input=self.normalize_literal_input(literal_input)
        return self.predicate(Gt(input))

    def __lt__(self, literal_input) -> QueryPredicates:
        if literal_input is None:
            return QueryPredicates()
        input=self.normalize_literal_input(literal_input)
        return self.predicate(Lt(input))

    def __ge__(self, literal_input) -> QueryPredicates:
        if literal_input is None:
            return QueryPredicates()
        input=self.normalize_literal_input(literal_input)
        return self.predicate(Gte(input)) 

    def __le__(self, literal_input) -> QueryPredicates:
        if literal_input is None:
            return QueryPredicates()
        input=self.normalize_literal_input(literal_input)
        return self.predicate(Lte(input)) 

    @suppress_warning
    def __eq__(self, input) -> QueryPredicates:
        if input is None:
            return QueryPredicates()
        
        if isinstance(input, Enum):
            return self.is_enum(input)
        
        if isinstance(input, Expression):
            return self.predicate(Eq(input))
        
        input=self.normalize_literal_input(input)        
        return QueryPredicates({self.get_query_name(): input})

    @suppress_warning
    def __ne__(self, literal_input) -> QueryPredicates:
        if literal_input is None:
            return QueryPredicates()
        
        input=self.normalize_literal_input(literal_input)
        return self.predicate(Ne(input)) 

    def is_in(self, literal_input) -> QueryPredicates:
        if literal_input is None:
            return QueryPredicates()

        input=self.normalize_literal_input(literal_input)
        assert isinstance(input, list)
        return self.predicate(In(input)) 
    
    def is_not_in(self, literal_input) -> QueryPredicates:
        if literal_input is None:
            return QueryPredicates()
        input=self.normalize_literal_input(literal_input)
        assert isinstance(input, list)
        return self.predicate(NotIn(input))
    
    def is_all(self, literal_input) -> QueryPredicates:
        if literal_input is None:
            return QueryPredicates()
        
        input=self.normalize_literal_input(literal_input)
        assert isinstance(input, list)
        return self.predicate(All(input))
    
    def is_not_all(self, literal_input) -> QueryPredicates:
        if literal_input is None:
            return QueryPredicates()

        input=self.normalize_literal_input(literal_input)
        assert isinstance(input, list)
        return self.predicate(NotAll(input))
    
    def is_within(self, input: Optional[TimeQuery | TimeFrame]):
        if input is None:
            return QueryPredicates()
        
        if isinstance(input, TimeFrame):
            time_query = TimeQuery().in_frame(input)
        else:
            time_query = input

        assert isinstance(time_query, TimeQuery)
        return self.predicate(time_query.for_fld(self.name))
    
    def is_enum(self, literal_input: Enum) -> QueryPredicates:
        if (literal_input.value == "ANY"):
            return QueryPredicates()
        return QueryPredicates({self.get_query_name(): literal_input.value})
    
    def is_false(self) -> QueryPredicates:
        return QueryPredicates({self.get_query_name(): False})
    
    def is_true(self) -> QueryPredicates:
        return QueryPredicates({self.get_query_name(): True})
    
    def is_exists(self) -> QueryPredicates:
        return self.predicate(Exists(True))
    
    def is_not_exists(self) -> QueryPredicates:
        return self.predicate(Exists(False))
    
    def is_none_or_missing(self) -> QueryPredicates:
        return QueryPredicates({self.get_query_name(): None})
    
    def predicate(self, query_op: QueryOpExpression) -> QueryPredicates:
        return QueryPredicates({self.get_query_name(): query_op})


    