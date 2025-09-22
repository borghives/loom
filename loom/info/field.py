from enum import Enum
from functools import wraps
from loom.info.filter import Filter
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
    Time,
    QueryOpExpression,
)
from loom.time.timeframing import TimeFrame

def suppress_warning(func):
    """suppress pylance error :-/ Not proud of myself! Picking on a dumb kid who you like"""
    @wraps(func) 
    def wrapper(self, other):
        return func(self, other)
    return wrapper

class fld:
    def __init__(self, name: str):
        self.name = name

    def __gt__(self, other) -> Filter:
        return self.predicate(Gt(other))

    def __lt__(self, other) -> Filter:
        return self.predicate(Lt(other))

    def __ge__(self, other) -> Filter:
        return self.predicate(Gte(other)) 

    def __le__(self, other) -> Filter:
        return self.predicate(Lte(other)) 

    @suppress_warning
    def __eq__(self, other) -> Filter:
        if isinstance(other, Enum):
            return self.is_enum(other)

        return self.predicate(Eq(other)) 

    @suppress_warning
    def __ne__(self, other) -> Filter:
        return self.predicate(Ne(other)) 

    def is_in(self, other) -> Filter:
        return self.predicate(In(other)) 
    
    def is_not_in(self, other) -> Filter:
        return self.predicate(NotIn(other))
    
    def is_all(self, other) -> Filter:
        return self.predicate(All(other))
    
    def is_not_all(self, other) -> Filter:
        return self.predicate(NotAll(other))
    
    def is_within(self, other: Time | TimeFrame):
        if isinstance(other, TimeFrame):
            return self.predicate(Time().in_frame(other))
        
        return self.predicate(other)
    
    def is_enum(self, other: Enum) -> Filter:
        if (other.value == "ANY"):
            return Filter()
        return self.predicate(other.value)
    
    def is_exists(self) -> Filter:
        return self.predicate(Exists(True))
    
    def is_not_exists(self) -> Filter:
        return self.predicate(Exists(False))
    
    def is_none_or_missing(self) -> Filter:
        return Filter({self.name: None})
    
    def predicate(self, query_op: QueryOpExpression) -> Filter:
        return Filter({self.name: query_op})




    
    

        