from functools import wraps
from typing import Protocol, runtime_checkable
from loom.info.load import Filter

@runtime_checkable
class Expression(Protocol):
    """
    A protocol for expressions that can be converted to a MongoDB expression.
    """
    def express(self) -> dict:
        ...

def check_expr(func):
    """Decorator to express an expr"""
    @wraps(func)  # Preserves the original function's name and docstring
    def wrapper(self, other):
        if (isinstance(other, Expression)):
            other = other.express()
        return func(self, other)
    return wrapper

class fld:
    def __init__(self, name: str):
        self.name = name

    @check_expr
    def __gt__(self, other) -> Filter:
        return Filter({self.name: {"$gt": other}})

    @check_expr
    def __lt__(self, other) -> Filter:
        return Filter({self.name: {"$lt": other}})

    @check_expr
    def __ge__(self, other) -> Filter:
        return Filter({self.name: {"$gte": other}})

    @check_expr
    def __le__(self, other) -> Filter:
        return Filter({self.name: {"$lte": other}})

    @check_expr
    def __eq__(self, other) -> Filter:
        return Filter({self.name: other})

    @check_expr
    def __ne__(self, other) -> Filter:
        return Filter({self.name: {"$ne": other}})
    
    @check_expr
    def is_in(self, other) -> Filter:
        return Filter({self.name: {"$in": other}})
    
    @check_expr
    def is_not_in(self, other) -> Filter:
        return Filter({self.name: {"$nin": other}})
    
    def is_exists(self) -> Filter:
        return Filter({self.name: {"$exists": True}})
    
    def is_not_exists(self) -> Filter:
        return Filter({self.name: {"$exists": False}})




    
    

        