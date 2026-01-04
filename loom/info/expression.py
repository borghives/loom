from typing import List
import email.contentmanager
from abc import ABC, abstractmethod
from typing import Dict, Optional, Any
from pydantic.fields import FieldInfo

from loom.info.model import NormalizeQueryInput
from loom.info.util import coalesce

class ExpressionDriver:
    """
    Handles the conversion of abstract `Expression` objects into concrete MongoDB queries.

    This class is responsible for:
    - Marshalling `Expression` objects into their MongoDB representation.
    - Resolving field names to their database aliases.
    - Applying any query-time value transformations associated with a model field.
    """

    def __init__(self, model_fields: dict[str, FieldInfo]):
        self.model_fields = model_fields

    def get_alias(self, field_name: str) -> str:
        field_info = self.model_fields.get(field_name)
        if field_info is None:
            return field_name
        return field_info.alias or field_name
    
    def get_transformers(self, field_name: str) -> list:
        field_info = self.model_fields.get(field_name)
        if field_info is None:
            return []
        metadata = field_info.metadata
        return [item for item in metadata if isinstance(item, NormalizeQueryInput)]

    def marshal(self, value):
        if isinstance(value, Expression):
            return value.express(self)
        if isinstance(value, dict):
            return {self.marshal(k): self.marshal(v) for k, v in value.items()}
        if isinstance(value, list):
            return [self.marshal(v) for v in value if v is not None]
        return value

class Expression(ABC):
    """
    An abstract representation of an object that can be converted into a MongoDB expression.

    This serves as the base class for various query components, such as filters,
    sort operators, and field predicates.
    """

    @property
    @abstractmethod
    def repr_value(self) -> Any:
        """
        Returns the raw representation of the expression before final marshalling.
        
        This property should return the internal state of the expression as a
        Python primitive (dict, list, value) that the `ExpressionDriver` will
        recursively traverse and resolve (e.g. resolving `FieldName` to aliases).
        """
        pass
    
    def express(self, driver: Optional[ExpressionDriver] = None):
        if driver is None:
            driver = ExpressionDriver({})
        
        return driver.marshal(self.repr_value) if driver else self.repr_value
    
    def is_empty(self):
        value = self.repr_value
        if value is None:
            return True
        
        if isinstance(value, dict):
            return len(value) == 0
        
        if isinstance(value, list):
            return len(value) == 0
    
        if isinstance(value, Expression):
            return value.is_empty()
        
        return len(value) == 0


class FieldName(Expression):
    """
    An expression representing a field name that can be resolved to a database alias.
    """
    def __init__ (self, field_name: str):
        self.field_name = field_name

    @property
    def repr_value(self):
        return self.field_name

    def express(self, driver: Optional[ExpressionDriver] = None):
        return driver.get_alias(self.repr_value) if driver else self.repr_value
    
class FieldPath(Expression):
    """
    An expression representing a field path used in aggregation expressions.

    Field paths are prefixed with a `$` sign in MongoDB aggregation pipelines.
    """
    def __init__ (self, field_name: str ):
        self.field_name = field_name

    @property
    def repr_value(self):
        return f"${self.field_name}"

    def express(self, driver: Optional[ExpressionDriver] = None):
        field_alias = driver.get_alias(self.field_name) if driver else self.field_name
        return f"${field_alias}"
        
class LiteralInput(Expression):
    """
    An expression representing a literal value used in a query predicate.

    This wrapper allows query-time transformations to be applied to the value
    based on the model field's annotations.
    """
    def __init__ (self, literal_input):
        self.literal_input = literal_input
        self.linked_field_name = ""

    def for_fld(self, field_name: str) -> "LiteralInput":
        self.linked_field_name = field_name
        return self

    @property
    def repr_value(self):
        return self.literal_input

    def express(self, driver: Optional[ExpressionDriver] = None):
        if driver is None or not self.linked_field_name:
            return self.repr_value
        
        transformers = driver.get_transformers(self.linked_field_name)
        return coalesce(self.repr_value, transformers)

def to_expr(input: Expression | str | int) -> Expression:
    if isinstance(input, str):
        return FieldPath(input)
    if isinstance(input, int):
        return LiteralInput(input)
    assert isinstance(input, Expression)
    return input

class OpExpression(Expression):
    @classmethod
    def of(cls, *inputs: Expression | str | int, **kwargs) -> "OpExpression":
        """
        Creates an instance of this class from a list of expressions or strings.
        """
        input_exprs = [to_expr(input) for input in inputs]
        return cls(*input_exprs, **kwargs)

         

class AccOpExpression(Expression):
    @classmethod
    def of(cls, *inputs: Expression | str | int, **kwargs) -> "AccOpExpression":
        """
        Creates an instance of this class from a list of expressions or strings.
        """
        input_exprs = [to_expr(input) for input in inputs]
        return cls(*input_exprs, **kwargs)

class MapExpression(Expression) :
    def __init__(self, accumulate: Optional[Dict] = None) -> None:
        self._value = accumulate if accumulate is not None else {}

    @property
    def repr_value(self):
        return self._value
    
    def __or__(self, other):
        """
        Combines this filter with another
        """
        if other is None:
            return self

        cls = self.__class__

        if not isinstance(other, cls):
            other = cls(other)

        if self.is_empty():
            return other
        
        if other.is_empty():
            return self

        self_clauses = self.repr_value
        other_clauses = other.repr_value

        assert isinstance(self_clauses, dict)
        assert isinstance(other_clauses, dict)
        combined = self_clauses | other_clauses
                
        return cls(combined)

# #output for field
# class GroupAccumulators(MapExpression):
#     """
#     An expression that represents a MongoDB query predicate (the part of a `find`
#     operation that selects documents).

#     Accumulator can be combined using with `|` operators.

#     ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/group/#std-label-accumulators-group
#     """
#     def __init__(self, accumulate: Optional[Dict] = None) -> None:
#         super().__init__(accumulate)

class FieldSpecification (MapExpression):
    """
    for group by:
    ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/group/#std-label-accumulators-group

    for project
    ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/project/
    """
    def __init__(self, specification: Optional[Dict] = None) :
        super().__init__(specification)


def combine_field_specifications(*specifications: FieldSpecification | dict):
    combined: Optional[FieldSpecification | dict] = None
    for specification in specifications:
        if combined is None:
            combined = specification
        else:
            if isinstance(combined, dict) and isinstance(specification, FieldSpecification):
                combined = FieldSpecification(combined)
            
            if isinstance(combined, FieldSpecification):
               if isinstance(specification, dict):
                   specification = FieldSpecification(specification)
               combined |= specification
            elif isinstance(combined, dict):
                assert isinstance(specification, dict)
                combined |= specification
                    
    return combined


class GroupExpression(Expression):
    def __init__(self, key: Expression | None):
        self.key = key
        self.accumulators : Optional[FieldSpecification] = None

    @property
    def repr_value(self) -> dict:
        group_expr  : dict = {"_id" : self.key}
        if self.accumulators:
            value = self.accumulators.repr_value
            assert isinstance(value, dict)
            group_expr.update(value)

        return group_expr
    
    def with_acc(self, accumulators: FieldSpecification) -> "GroupExpression":
        self.accumulators = accumulators
        return self
    
