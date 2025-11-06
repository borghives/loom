from abc import ABC, abstractmethod
from typing import Optional
from pydantic.fields import FieldInfo

from loom.info.model import NormalizeQueryInput


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
            return [self.marshal(v) for v in value]
        return value

class Expression(ABC):
    """
    An abstract representation of an object that can be converted into a MongoDB expression.

    This serves as the base class for various query components, such as filters,
    sort operators, and field predicates.
    """

    @property
    @abstractmethod
    def repr_value(self):
        pass
    
    def express(self, driver: Optional[ExpressionDriver] = None):
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
