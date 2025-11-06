from typing import Optional
from loom.info.expression import Expression, ExpressionDriver
from loom.info.util import coalesce

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
        
class PredicateInput(Expression):
    """
    An expression representing a literal value used in a query predicate.

    This wrapper allows query-time transformations to be applied to the value
    based on the model field's annotations.
    """
    def __init__ (self, field_name: str, literal_input):
        self.field_name = field_name
        self.literal_input = literal_input

    @property
    def repr_value(self):
        return self.literal_input

    def express(self, driver: Optional[ExpressionDriver] = None):
        if driver is None:
            return self.repr_value
        
        transformers = driver.get_transformers(self.field_name)
        return coalesce(self.repr_value, transformers)
