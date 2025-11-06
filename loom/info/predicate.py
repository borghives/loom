from typing import Optional
from loom.info.expression import Expression, ExpressionDriver

class PredicateName(Expression):
    def __init__ (self, field_name: str):
        self.field_name = field_name

    @property
    def value(self):
        return self.field_name

    def express(self, driver: Optional[ExpressionDriver] = None):
        return driver.get_alias(self.value) if driver else self.value
        
class PredicateInput(Expression):
    def __init__ (self, field_name: str, literal_input):
        self.field_name = field_name
        self.literal_input = literal_input

    @property
    def value(self):
        return self.literal_input

    def express(self, driver: Optional[ExpressionDriver] = None):
        if driver is None:
            return self.value
        
        transformers = driver.get_transformers(self.field_name)
        return coalesce(self.value, transformers)

def coalesce(value, transformers: list):
    """Applies a list of transformers sequentially to a value."""
    for transformer in transformers:
        value = transformer(value)
    return value