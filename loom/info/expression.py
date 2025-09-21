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
    
    def __and__(self, other):
        if (other is None):
            return self
        
        value = self.wrap(other)
        assert(isinstance(value, Expression))

        if (value.is_empty()):
            return self

        if (self.is_empty()):
            return other

        retval = self.wrap(self.express() | value.express())

        assert(retval is not None)
        return retval