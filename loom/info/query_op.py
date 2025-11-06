
from collections import UserList
from dataclasses import dataclass
from datetime import datetime
from typing import Optional
from loom.info.expression import Expression, LiteralInput

from loom.time.timeframing import TimeFrame
from loom.time.util import to_utc_aware



class QueryOpExpression(Expression):
    """
    A MongoDB query operator expression.
    """

    pass


class In(UserList, QueryOpExpression):
    """
    A match directive that creates a `$in` operator.
    """

    @property
    def repr_value(self):
        return {"$in": self.data}


class NotIn(UserList, QueryOpExpression):
    """
    A match directive that creates a `$nin` operator.
    """

    @property
    def repr_value(self):
        return {"$nin": self.data}

class All(UserList, QueryOpExpression):
    """
    A match directive that creates a `$all` operator.
    """

    @property
    def repr_value(self):
        return {"$all": self.data}

class Not(QueryOpExpression):
    """
    A match directive that creates a `$not` operator.
    """

    def __init__(self, value):
        self._value = value

    @property
    def repr_value(self):
        return {"$not": self._value}

class NotAll(UserList, QueryOpExpression):
    """
    A match directive that creates a not `$all` operator.
    """

    @property
    def repr_value(self):
        return Not(All(self.data))


class Gt(QueryOpExpression):
    """
    A match directive that creates a `$gt` operator.
    """

    def __init__(self, value):
        self._value = value

    @property
    def repr_value(self):
        return {"$gt": self._value}

class Gte(QueryOpExpression):
    """
    A match directive that creates a `$gte` operator.
    """

    def __init__(self, value):
        self._value = value

    @property
    def repr_value(self):
        return {"$gte" : self._value}


class Lt(QueryOpExpression):
    """
    A match directive that creates a `$lt` operator.
    """

    def __init__(self, value):
        self._value = value

    @property
    def repr_value(self):
        return {"$lt" : self._value}


class Lte(QueryOpExpression):
    """
    A match directive that creates a `$lte` filter.
    """

    def __init__(self, value):
        self._value = value

    @property
    def repr_value(self):
        return {"$lte" : self._value}


class Eq(QueryOpExpression):
    """
    A match directive that creates a `$eq` filter.
    """

    def __init__(self, value):
        self._value = value

    @property
    def repr_value(self):
        return {"$eq" : self._value}
    
class Ne(QueryOpExpression):
    """
    A match directive that creates a `$neq` filter.
    """

    def __init__(self, value):
        self._value = value

    @property
    def repr_value(self):
        return {"$ne" : self._value}

class Exists(QueryOpExpression):
    def __init__(self, value: bool):
        self._value = value

    @property
    def repr_value(self):
        return {"exists": self._value}

@dataclass
class TimeQuery(QueryOpExpression):
    """
    A time match description that creates a time-based query operator expression.
    """
    after_t: Optional[datetime] = None
    after_incl: bool = False
    before_t: Optional[datetime] = None
    before_incl: bool = False

    field_name: str = ""

    def for_fld(self, field_name: str) -> "TimeQuery":
        self.field_name = field_name
        return self
    
    @property
    def repr_value(self):
        time_match = {}
        if self.after_t:
            time_match["$gte" if self.after_incl else "$gt"] = LiteralInput(self.after_t).for_fld(self.field_name)

        if self.before_t:
            time_match["$lte" if self.before_incl else "$lt"] = LiteralInput(self.before_t).for_fld(self.field_name)
        return time_match
    
    def is_empty(self):
        return self.after_t is None and self.before_t is None
    
    def after(self, time : datetime) -> "TimeQuery":
        time = to_utc_aware(time)
        if (self.after_t is None or time > self.after_t) :
            self.after_t = time
        self.after_incl = False
        return self
    
    def before(self, time : datetime) -> "TimeQuery":
        time = to_utc_aware(time)
        if (self.before_t is None or time < self.before_t) :
            self.before_t = time
        self.before_incl = False
        return self
    
    def iafter(self, time : datetime) -> "TimeQuery":
        self.after(time)
        self.after_incl = True
        return self
    
    def ibefore(self, time : datetime) -> "TimeQuery":
        self.before(time)
        self.before_incl = True
        return self
    
    def period(self, floor: datetime, ceiling: datetime) -> "TimeQuery":
        self.iafter(floor)
        self.before(ceiling)
        return self
    
    def in_frame(self, frame: TimeFrame) -> "TimeQuery":
        return self.period(frame.floor, frame.ceiling)

class And(UserList, QueryOpExpression):
    """
    A match directive that creates an `$and` operator.
    It takes a list of filter expressions.
    """

    @property
    def repr_value(self):
        return {"$and": self.data}

class Or(UserList, QueryOpExpression):
    """
    A match directive that creates an `$or` operator.
    It takes a list of filter expressions.
    """
    @property
    def repr_value(self):
        return {"$or": self.data}