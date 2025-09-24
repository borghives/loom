from abc import abstractmethod
from collections import UserList
from dataclasses import dataclass
from datetime import datetime
from typing import Optional
from loom.info.expression import Expression, marshal_expression
from loom.time.timeframing import TimeFrame
from loom.time.util import to_utc_aware


class QueryOpExpression(Expression):
    """
    A MongoDB query operator expression.
    """

    @abstractmethod
    def express(self) -> dict:
        pass


class In(UserList, QueryOpExpression):
    """
    A match directive that creates a `$in` operator.
    """

    def express(self) -> dict:
        return {"$in": self.data}


class NotIn(UserList, QueryOpExpression):
    """
    A match directive that creates a `$nin` operator.
    """

    def express(self) -> dict:
        return {"$nin": self.data}

class All(UserList, QueryOpExpression):
    """
    A match directive that creates a `$all` operator.
    """

    def express(self) -> dict:
        return {"$all": self.data}

class Not(QueryOpExpression):
    """
    A match directive that creates a `$not` operator.
    """

    def __init__(self, value):
        self.value = value

    def express(self) -> dict:
        if isinstance(self.value, QueryOpExpression):
            return {"$not": self.value.express()}
        
        return {"$not": self.value}

class NotAll(UserList, QueryOpExpression):
    """
    A match directive that creates a not `$all` operator.
    """

    def express(self) -> dict:
        return Not(All(self.data)).express()


class Gt(QueryOpExpression):
    """
    A match directive that creates a `$gt` operator.
    """

    def __init__(self, value):
        self.value = value

    def express(self) -> dict:
        return {"$gt": self.value}


class Gte(QueryOpExpression):
    """
    A match directive that creates a `$gte` operator.
    """

    def __init__(self, value):
        self.value = value

    def express(self) -> dict:
        return {"$gte": self.value}


class Lt(QueryOpExpression):
    """
    A match directive that creates a `$lt` operator.
    """

    def __init__(self, value):
        self.value = value

    def express(self) -> dict:
        return {"$lt": self.value}


class Lte(QueryOpExpression):
    """
    A match directive that creates a `$lte` filter.
    """

    def __init__(self, value):
        self.value = value

    def express(self) -> dict:
        return {"$lte": self.value}


class Eq(QueryOpExpression):
    """
    A match directive that creates a `$eq` filter.
    """

    def __init__(self, value):
        self.value = value

    def express(self) -> dict:
        return {"$eq": self.value}
    
class Ne(QueryOpExpression):
    """
    A match directive that creates a `$neq` filter.
    """

    def __init__(self, value):
        self.value = value

    def express(self) -> dict:
        return {"$ne": self.value}

class Exists(QueryOpExpression):
    """
    A match directive that creates a `$neq` filter.
    """

    def __init__(self, value: bool):
        self.value = value

    def express(self) -> dict:
        return {"$exists": self.value}

@dataclass
class Time(QueryOpExpression):
    """
    A time match description that creates a time-based query operator expression.
    """

    after_t: Optional[datetime] = None
    after_incl: bool = False
    before_t: Optional[datetime] = None
    before_incl: bool = False

    @classmethod
    def wrap(cls, value):
        if isinstance(value, Time):
            return value
        
        return None

    def express(self) -> dict:
        time_match = {}

        if self.after_t:
            time_match["$gte" if self.after_incl else "$gt"] = self.after_t

        if self.before_t:
            time_match["$lte" if self.before_incl else "$lt"] = self.before_t

        return time_match
    
    def after(self, time : datetime) -> "Time":
        time = to_utc_aware(time)
        if (self.after_t is None or time > self.after_t) :
            self.after_t = time
        self.after_incl = False
        return self
    
    def before(self, time : datetime) -> "Time":
        time = to_utc_aware(time)
        if (self.before_t is None or time < self.before_t) :
            self.before_t = time
        self.before_incl = False
        return self
    
    def iafter(self, time : datetime) -> "Time":
        self.after(time)
        self.after_incl = True
        return self
    
    def ibefore(self, time : datetime) -> "Time":
        self.before(time)
        self.before_incl = True
        return self
    
    def period(self, floor: datetime, ceiling: datetime) -> "Time":
        self.iafter(floor)
        self.before(ceiling)
        return self
    
    def in_frame(self, frame: TimeFrame) -> "Time":
        return self.period(frame.floor, frame.ceiling)

class And(UserList, QueryOpExpression):
    """
    A match directive that creates an `$and` operator.
    It takes a list of filter expressions.
    """
    def express(self) -> dict:
        return {"$and": [marshal_expression(item) for item in self.data]}

class Or(UserList, QueryOpExpression):
    """
    A match directive that creates an `$or` operator.
    It takes a list of filter expressions.
    """
    def express(self) -> dict:
        return {"$or": [marshal_expression(item) for item in self.data]}