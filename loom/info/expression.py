from abc import ABC, abstractmethod
from typing import Optional
from pydantic.fields import FieldInfo

from loom.info.model import NormalizeQueryInput

class ExpressionDriver:
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
    An representation that can be converted to a MongoDB expression.
    """

    @property
    @abstractmethod
    def value(self):
        pass
    
    def express(self, driver: Optional[ExpressionDriver] = None):
        return driver.marshal(self.value) if driver else self.value
    
    def is_empty(self):
        value = self.value
        if value is None:
            return True
        
        if isinstance(value, dict):
            return len(value) == 0
        
        if isinstance(value, list):
            return len(value) == 0
    
        if isinstance(value, Expression):
            return value.is_empty()
        
        return len(value) == 0
