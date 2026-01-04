from typing import Optional
from loom.info.expression import Expression
from datetime import datetime
from loom.time.timeframing import TimeFrame
from loom.time.util import to_utc_aware
from loom.info.expression import OpExpression


class ToInt(OpExpression):
    def __init__(self, input: Expression) -> None:
        self.input = input

    @property
    def repr_value(self):
        return {
            "$toInt": self.input
        }

class ToDouble(OpExpression):
    def __init__(self, input: Expression) -> None:
        self.input = input

    @property
    def repr_value(self):
        return {
            "$toDouble": self.input
        }

class ToLong(OpExpression):
    def __init__(self, input: Expression) -> None:
        self.input = input

    @property
    def repr_value(self):
        return {
            "$toLong": self.input
        }

class ToDecimal(OpExpression):
    def __init__(self, input: Expression) -> None:
        self.input = input

    @property
    def repr_value(self):
        return {
            "$toDecimal": self.input
        }

class Abs(OpExpression):
    def __init__(self, input: Expression) -> None:
        self.input = input

    @property
    def repr_value(self):
        return {
            "$abs": self.input
        }

class Subtract(OpExpression):
    def __init__(self, input1: Expression, input2: Expression) -> None:
        self.input1 = input1
        self.input2 = input2

    @property
    def repr_value(self):
        return {
            "$subtract": [self.input1, self.input2]
        }

class Add(OpExpression):
    def __init__(self, input1: Expression, input2: Expression) -> None:
        self.input1 = input1
        self.input2 = input2

    @property
    def repr_value(self):
        return {
            "$add": [self.input1, self.input2]
        }

class DateToString(OpExpression):
    def __init__(self, input: Expression, format: str, timezone: Optional[str] = None) -> None:
        self.input = input
        self.format = format
        self.timezone = timezone
        
    @property
    def repr_value(self):
        date_to_string = {
            "format": self.format,
            "date": self.input,
        }

        if self.timezone:
            date_to_string["timezone"] = self.timezone

        return {
            "$dateToString": date_to_string
        }

def divide(numerator, denominator) -> dict:
    """
    Returns a MongoDB `$divide` operator structure.

    Args:
        numerator: The numerator.
        denominator: The denominator.

    Returns:
        dict: A MongoDB `$divide` operator structure.
    """
    return {"$divide": [numerator, denominator]}


def multiply(a, b) -> dict:
    """
    Returns a MongoDB `$multiply` operator structure.

    Args:
        a: The first number.
        b: The second number.

    Returns:
        dict: A MongoDB `$multiply` operator structure.
    """
    return {"$multiply": [a, b]}

def add(a, b) -> dict:
    """
    Returns a MongoDB `$add` operator structure.

    Args:
        a: The first number.
        b: The second number.

    Returns:
        dict: A MongoDB `$add` operator structure.
    """
    return {"$add": [a, b]}


def sanitize_number(expr, default: int = 0) -> dict:
    """
    Returns a MongoDB `$ifNull` operator structure to default null numbers to 0.

    Args:
        expr: The expression to sanitize.

    Returns:
        dict: A MongoDB `$ifNull` operator structure.
    """
    return {"$ifNull": [expr, default]}


def to_double(expr) -> dict:
    """
    Returns a MongoDB `$convert` operator structure to convert a value to a
    double.

    Args:
        expr: The expression to convert.

    Returns:
        dict: A MongoDB `$convert` operator structure.
    """
    return {
        "$convert": {"input": expr, "to": "double", "onError": None, "onNull": None}
    }


def to_int(expr) -> dict:
    """
    Returns a MongoDB `$convert` operator structure to convert a value to an
    integer.

    Args:
        expr: The expression to convert.

    Returns:
        dict: A MongoDB `$convert` operator structure.
    """
    return {"$convert": {"input": expr, "to": "int", "onError": None, "onNull": None}}


def to_upper(expr) -> dict:
    """
    Returns a MongoDB `$toUpper` operator structure.

    Args:
        expr: The expression to convert.

    Returns:
        dict: A MongoDB `$toUpper` operator structure.
    """
    return {"$toUpper": expr}


def to_lower(expr) -> dict:
    """
    Returns a MongoDB `$toLower` operator structure.

    Args:
        expr: The expression to convert.

    Returns:
        dict: A MongoDB `$toLower` operator structure.
    """
    return {"$toLower": expr}


def to_date_alignment(expr, hour: int) -> dict:
    """
    Returns a MongoDB `$toDate` operator structure to align a date to a
    specific hour.

    Args:
        expr: The expression to convert.
        hour (int): The hour to align to.

    Returns:
        dict: A MongoDB `$toDate` operator structure.
    """
    if hour < 0 or hour > 23:
        raise ValueError("Hour must be between 0 and 23")

    return {"$toDate": {"$concat": [f"{expr}", f"T{hour:02}:00:00.000Z"]}}

def m_timeframe(windowframe: TimeFrame) -> dict:
    return m_period(windowframe.floor, windowframe.ceiling)

def m_period(floor:datetime, ceiling:datetime) -> dict:
    return  {
                "$gte": to_utc_aware(floor),
                "$lt": to_utc_aware(ceiling)
    }
