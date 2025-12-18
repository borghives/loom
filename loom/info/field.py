
from loom.info.acc_op import Last
from loom.info.acc_op import First
from loom.info.acc_op import ArrayElemAt
from enum import Enum
from typing import Optional

from loom.info.acc_op import Avg, Max, Median, Min, Sum, Percentile
from loom.info.expression import Expression, FieldPath, FieldSpecification, LiteralInput, FieldName
from loom.info.filter import QueryPredicates
from loom.info.op import sanitize_number
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

    def __eq__(self, input) -> QueryPredicates: # type: ignore[override]
        if input is None:
            return QueryPredicates()
        
        if isinstance(input, Enum):
            return self.is_enum(input)
        
        if isinstance(input, Expression):
            return self.predicate(Eq(input))
        
        input=self.normalize_literal_input(input)        
        return QueryPredicates({self.get_query_name(): input})

    def __ne__(self, literal_input) -> QueryPredicates: # type: ignore[override]
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
    
    def is_exists(self, exists: Optional[bool] = True) -> QueryPredicates:
        if exists is None:
            return QueryPredicates()
        return self.predicate(Exists(exists))
    
    def is_not_exists(self) -> QueryPredicates:
        return self.predicate(Exists(False))
    
    def is_none_or_missing(self) -> QueryPredicates:
        return QueryPredicates({self.get_query_name(): None})
    
    def with_sane_num(self, default: int = 0) -> FieldSpecification:
        return self.with_(sanitize_number(FieldPath(self.name), default=default))

    def with_median(self, input: Expression | str) -> FieldSpecification:
        if isinstance(input, str):
            input = FieldPath(input)
        assert isinstance(input, Expression)
        return self.with_( Median(input))

    def with_percentile(self, input: Expression | str, p: list[float]) -> FieldSpecification:
        if isinstance(input, str):
            input = FieldPath(input)
        assert isinstance(input, Expression)
        return self.with_( Percentile(input, p=p))

    def with_elem_at(self, input: Expression | str, index: int) -> FieldSpecification:
        if isinstance(input, str):
            input = FieldPath(input)
        assert isinstance(input, Expression)
        return self.with_( ArrayElemAt(input, index))

    def with_first(self, input: Expression | str) -> FieldSpecification:
        if isinstance(input, str):
            input = FieldPath(input)
        assert isinstance(input, Expression)
        return self.with_( First(input))
    
    def with_last(self, input: Expression | str) -> FieldSpecification:
        if isinstance(input, str):
            input = FieldPath(input)
        assert isinstance(input, Expression)
        return self.with_( Last(input))
    
    def with_sum(self, input: Expression | str | int) -> FieldSpecification:
        if isinstance(input, str):
            input = FieldPath(input)
        assert isinstance(input, Expression)
        return self.with_( Sum(input))
    
    def with_avg(self, input: Expression | str) -> FieldSpecification:
        if isinstance(input, str):
            input = FieldPath(input)
        assert isinstance(input, Expression)
        return self.with_( Avg(input))
    
    def with_min(self, input: Expression | str) -> FieldSpecification:
        if isinstance(input, str):
            input = FieldPath(input)
        assert isinstance(input, Expression)
        return self.with_( Min(input))
    
    def with_max(self, input: Expression | str) -> FieldSpecification:
        if isinstance(input, str):
            input = FieldPath(input)
        assert isinstance(input, Expression)
        return self.with_( Max(input))
    
    def with_(self, spec : int | dict | str |Expression = 1) -> FieldSpecification:
        if isinstance(spec, str):
            spec = FieldPath(spec)
        return FieldSpecification({self.get_query_name(): spec})
    
    def with_out(self) -> FieldSpecification:
        return self.with_(0)
    
    def predicate(self, query_op: QueryOpExpression) -> QueryPredicates:
        return QueryPredicates({self.get_query_name(): query_op})
    


    