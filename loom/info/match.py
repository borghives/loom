
from collections import UserList
from dataclasses import dataclass
from datetime import datetime
from typing import Optional
from loom.info.load import MatchDirective
from loom.time.util import to_utc_aware


class In(UserList, MatchDirective):
    """
    A match directive that creates a `$in` filter.
    """

    def to_filter(self) -> dict:
        return {"$in": self.data}


class NotIn(UserList, MatchDirective):
    """
    A match directive that creates a `$nin` filter.
    """

    def to_filter(self) -> dict:
        return {"$nin": self.data}


class All(UserList, MatchDirective):
    """
    A match directive that creates a `$all` filter.
    """

    def to_filter(self) -> dict:
        return {"$all": self.data}


class NotAll(UserList, MatchDirective):
    """
    A match directive that creates a not `$all` filter.
    """

    def to_filter(self) -> dict:
        return {"$not": {"$all": self.data}}


class Gt(MatchDirective):
    """
    A match directive that creates a `$gt` filter.
    """

    def __init__(self, value):
        self.value = value

    def to_filter(self) -> dict:
        return {"$gt": self.value}


class Gte(MatchDirective):
    """
    A match directive that creates a `$gte` filter.
    """

    def __init__(self, value):
        self.value = value

    def to_filter(self) -> dict:
        return {"$gte": self.value}


class Lt(MatchDirective):
    """
    A match directive that creates a `$lt` filter.
    """

    def __init__(self, value):
        self.value = value

    def to_filter(self) -> dict:
        return {"$lt": self.value}


class Lte(MatchDirective):
    """
    A match directive that creates a `$lte` filter.
    """

    def __init__(self, value):
        self.value = value

    def to_filter(self) -> dict:
        return {"$lte": self.value}


class Eq(MatchDirective):
    """
    A match directive that creates a `$eq` filter.
    """

    def __init__(self, value):
        self.value = value

    def to_filter(self) -> dict:
        return {"$eq": self.value}


# class InTimeFrame(MatchDirective):
#     """
#     A match directive that creates a time frame filter.
#     """

#     def __init__(self, timeframe: TimeFrame):
#         self.timeframe = timeframe

#     def to_filter(self) -> dict:
#         return {
#             "$gte": to_utc_aware(self.timeframe.floor),
#             "$lt": to_utc_aware(self.timeframe.ceiling),
#         }


@dataclass
class Time(MatchDirective):
    """
    A match directive that creates a time-based filter.
    """

    after_t: Optional[datetime] = None
    after_incl: bool = False
    before_t: Optional[datetime] = None
    before_incl: bool = False

    def to_filter(self) -> dict:
        time_match = {}

        if self.after_t:
            time_match["$gte" if self.after_incl else "$gt"] = self.after_t

        if self.before_t:
            time_match["$lte" if self.before_incl else "$lt"] = self.before_t

        return time_match
    
    def after(self, time : datetime) -> "Time":
        self.after_t = to_utc_aware(time)
        self.after_incl = False
        return self
    
    def before(self, time : datetime) -> "Time":
        self.before_t = to_utc_aware(time)
        self.before_incl = False
        return self
    
    def iafter(self, time : datetime) -> "Time":
        self.after_t = to_utc_aware(time)
        self.after_incl = True
        return self
    
    def ibefore(self, time : datetime) -> "Time":
        self.before_t = to_utc_aware(time)
        self.before_incl = True
        return self