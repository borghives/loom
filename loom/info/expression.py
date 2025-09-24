from abc import ABC, abstractmethod

class Expression(ABC):
    """
    An representation that can be converted to a MongoDB expression.
    """
    @abstractmethod
    def express(self) -> dict:
        pass

    @classmethod
    def wrap(cls, value):
        raise NotImplementedError()
    
    def is_empty(self):
        value = self.express()
        return value is None or len(value) == 0

def marshal_expression(value):
    """
    Parses a match directive.

    Args:
        value: The value to parse.

    Returns:
        The parsed value.
    """
    if isinstance(value, Expression):
        return value.express()
    if isinstance(value, dict):
        return {k: marshal_expression(v) for k, v in value.items()}
    if isinstance(value, list):
        return [marshal_expression(v) for v in value]
    return value