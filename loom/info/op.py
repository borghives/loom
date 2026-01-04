from typing import Optional, List, Any, Union, Dict
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


def _date_op_repr(op: str, date: Expression, timezone: Optional[Expression] = None):
    if timezone:
        return {op: {"date": date, "timezone": timezone}}
    return {op: date}

class Multiply(OpExpression):
    def __init__(self, *inputs: Expression) -> None:
        self.inputs = inputs

    @property
    def repr_value(self):
        return {"$multiply": list(self.inputs)}

class Divide(OpExpression):
    def __init__(self, input1: Expression, input2: Expression) -> None:
        self.input1 = input1
        self.input2 = input2

    @property
    def repr_value(self):
        return {"$divide": [self.input1, self.input2]}

class Mod(OpExpression):
    def __init__(self, input1: Expression, input2: Expression) -> None:
        self.input1 = input1
        self.input2 = input2

    @property
    def repr_value(self):
        return {"$mod": [self.input1, self.input2]}

class Pow(OpExpression):
    def __init__(self, number: Expression, exponent: Expression) -> None:
        self.number = number
        self.exponent = exponent

    @property
    def repr_value(self):
        return {"$pow": [self.number, self.exponent]}

class Sqrt(OpExpression):
    def __init__(self, input: Expression) -> None:
        self.input = input

    @property
    def repr_value(self):
        return {"$sqrt": self.input}

class Exp(OpExpression):
    def __init__(self, input: Expression) -> None:
        self.input = input

    @property
    def repr_value(self):
        return {"$exp": self.input}

class Ln(OpExpression):
    def __init__(self, input: Expression) -> None:
        self.input = input

    @property
    def repr_value(self):
        return {"$ln": self.input}

class Log(OpExpression):
    def __init__(self, number: Expression, base: Expression) -> None:
        self.number = number
        self.base = base

    @property
    def repr_value(self):
        return {"$log": [self.number, self.base]}

class Log10(OpExpression):
    def __init__(self, input: Expression) -> None:
        self.input = input

    @property
    def repr_value(self):
        return {"$log10": self.input}

class Ceil(OpExpression):
    def __init__(self, input: Expression) -> None:
        self.input = input

    @property
    def repr_value(self):
        return {"$ceil": self.input}

class Floor(OpExpression):
    def __init__(self, input: Expression) -> None:
        self.input = input

    @property
    def repr_value(self):
        return {"$floor": self.input}

class Trunc(OpExpression):
    def __init__(self, input: Expression, place: Optional[Expression] = None) -> None:
        self.input = input
        self.place = place

    @property
    def repr_value(self):
        if self.place is not None:
             return {"$trunc": [self.input, self.place]}
        return {"$trunc": self.input}

class Round(OpExpression):
    def __init__(self, input: Expression, place: Optional[Expression] = None) -> None:
        self.input = input
        self.place = place

    @property
    def repr_value(self):
        if self.place is not None:
             return {"$round": [self.input, self.place]}
        return {"$round": self.input}

class Concat(OpExpression):
    def __init__(self, *inputs: Expression) -> None:
        self.inputs = inputs

    @property
    def repr_value(self):
        return {"$concat": list(self.inputs)}

class Substr(OpExpression):
    def __init__(self, string: Expression, start: Expression, length: Expression) -> None:
        self.string = string
        self.start = start
        self.length = length

    @property
    def repr_value(self):
        return {"$substr": [self.string, self.start, self.length]}

class SubstrCP(OpExpression):
    def __init__(self, string: Expression, code_point_index: Expression, code_point_count: Expression) -> None:
        self.string = string
        self.code_point_index = code_point_index
        self.code_point_count = code_point_count

    @property
    def repr_value(self):
        return {"$substrCP": [self.string, self.code_point_index, self.code_point_count]}

class ToLower(OpExpression):
    def __init__(self, input: Expression) -> None:
        self.input = input

    @property
    def repr_value(self):
        return {"$toLower": self.input}

class ToUpper(OpExpression):
    def __init__(self, input: Expression) -> None:
        self.input = input

    @property
    def repr_value(self):
        return {"$toUpper": self.input}

class StrLenBytes(OpExpression):
    def __init__(self, input: Expression) -> None:
        self.input = input

    @property
    def repr_value(self):
        return {"$strLenBytes": self.input}

class StrLenCP(OpExpression):
    def __init__(self, input: Expression) -> None:
        self.input = input

    @property
    def repr_value(self):
        return {"$strLenCP": self.input}

class Strcasecmp(OpExpression):
    def __init__(self, string1: Expression, string2: Expression) -> None:
        self.string1 = string1
        self.string2 = string2

    @property
    def repr_value(self):
        return {"$strcasecmp": [self.string1, self.string2]}

class ArrayElemAt(OpExpression):
    def __init__(self, array: Expression, index: Expression) -> None:
        self.array = array
        self.index = index

    @property
    def repr_value(self):
        return {"$arrayElemAt": [self.array, self.index]}

class ConcatArrays(OpExpression):
    def __init__(self, *arrays: Expression) -> None:
        self.arrays = arrays

    @property
    def repr_value(self):
        return {"$concatArrays": list(self.arrays)}

class Size(OpExpression):
    def __init__(self, input: Expression) -> None:
        self.input = input

    @property
    def repr_value(self):
        return {"$size": self.input}

class IsArray(OpExpression):
    def __init__(self, input: Expression) -> None:
        self.input = input

    @property
    def repr_value(self):
        return {"$isArray": self.input}

class In(OpExpression):
    def __init__(self, element: Expression, array: Expression) -> None:
        self.element = element
        self.array = array

    @property
    def repr_value(self):
        return {"$in": [self.element, self.array]}

class Year(OpExpression):
    def __init__(self, date: Expression, timezone: Optional[Expression] = None) -> None:
        self.date = date
        self.timezone = timezone

    @property
    def repr_value(self):
        return _date_op_repr("$year", self.date, self.timezone)

class Month(OpExpression):
    def __init__(self, date: Expression, timezone: Optional[Expression] = None) -> None:
        self.date = date
        self.timezone = timezone

    @property
    def repr_value(self):
        return _date_op_repr("$month", self.date, self.timezone)

class DayOfMonth(OpExpression):
    def __init__(self, date: Expression, timezone: Optional[Expression] = None) -> None:
        self.date = date
        self.timezone = timezone

    @property
    def repr_value(self):
        return _date_op_repr("$dayOfMonth", self.date, self.timezone)

class DayOfWeek(OpExpression):
    def __init__(self, date: Expression, timezone: Optional[Expression] = None) -> None:
        self.date = date
        self.timezone = timezone

    @property
    def repr_value(self):
        return _date_op_repr("$dayOfWeek", self.date, self.timezone)

class DayOfYear(OpExpression):
    def __init__(self, date: Expression, timezone: Optional[Expression] = None) -> None:
        self.date = date
        self.timezone = timezone

    @property
    def repr_value(self):
        return _date_op_repr("$dayOfYear", self.date, self.timezone)

class Hour(OpExpression):
    def __init__(self, date: Expression, timezone: Optional[Expression] = None) -> None:
        self.date = date
        self.timezone = timezone

    @property
    def repr_value(self):
        return _date_op_repr("$hour", self.date, self.timezone)

class Minute(OpExpression):
    def __init__(self, date: Expression, timezone: Optional[Expression] = None) -> None:
        self.date = date
        self.timezone = timezone

    @property
    def repr_value(self):
        return _date_op_repr("$minute", self.date, self.timezone)

class Second(OpExpression):
    def __init__(self, date: Expression, timezone: Optional[Expression] = None) -> None:
        self.date = date
        self.timezone = timezone

    @property
    def repr_value(self):
        return _date_op_repr("$second", self.date, self.timezone)

class Millisecond(OpExpression):
    def __init__(self, date: Expression, timezone: Optional[Expression] = None) -> None:
        self.date = date
        self.timezone = timezone

    @property
    def repr_value(self):
        return _date_op_repr("$millisecond", self.date, self.timezone)

class IsoDayOfWeek(OpExpression):
    def __init__(self, date: Expression, timezone: Optional[Expression] = None) -> None:
        self.date = date
        self.timezone = timezone

    @property
    def repr_value(self):
        return _date_op_repr("$isoDayOfWeek", self.date, self.timezone)

class IsoWeek(OpExpression):
    def __init__(self, date: Expression, timezone: Optional[Expression] = None) -> None:
        self.date = date
        self.timezone = timezone

    @property
    def repr_value(self):
        return _date_op_repr("$isoWeek", self.date, self.timezone)

class IsoWeekYear(OpExpression):
    def __init__(self, date: Expression, timezone: Optional[Expression] = None) -> None:
        self.date = date
        self.timezone = timezone

    @property
    def repr_value(self):
        return _date_op_repr("$isoWeekYear", self.date, self.timezone)

class Cond(OpExpression):
    def __init__(self, if_expr: Expression, then_expr: Expression, else_expr: Expression) -> None:
        self.if_expr = if_expr
        self.then_expr = then_expr
        self.else_expr = else_expr

    @property
    def repr_value(self):
        return {"$cond": {"if": self.if_expr, "then": self.then_expr, "else": self.else_expr}}

class IfNull(OpExpression):
    def __init__(self, input: Expression, replacement: Expression) -> None:
        self.input = input
        self.replacement = replacement

    @property
    def repr_value(self):
        return {"$ifNull": [self.input, self.replacement]}

class Switch(OpExpression):
    def __init__(self, branches: List[dict], default: Optional[Expression] = None) -> None:
        self.branches = branches
        self.default = default

    @property
    def repr_value(self):
        val: Dict[str, Any] = {"branches": self.branches}
        if self.default is not None:
            val["default"] = self.default
        return {"$switch": val}

class Type(OpExpression):
    def __init__(self, input: Expression) -> None:
        self.input = input

    @property
    def repr_value(self):
        return {"$type": self.input}

class Convert(OpExpression):
    def __init__(self, input: Expression, to: Union[str, Expression], on_error: Optional[Expression] = None, on_null: Optional[Expression] = None) -> None:
        self.input = input
        self.to = to
        self.on_error = on_error
        self.on_null = on_null

    @property
    def repr_value(self):
        val = {"input": self.input, "to": self.to}
        if self.on_error is not None:
            val["onError"] = self.on_error
        if self.on_null is not None:
            val["onNull"] = self.on_null
        return {"$convert": val}

class ToString(OpExpression):
    def __init__(self, input: Expression) -> None:
        self.input = input

    @property
    def repr_value(self):
        return {"$toString": self.input}

class ToBool(OpExpression):
    def __init__(self, input: Expression) -> None:
        self.input = input

    @property
    def repr_value(self):
        return {"$toBool": self.input}

class ToDate(OpExpression):
    def __init__(self, input: Expression) -> None:
        self.input = input

    @property
    def repr_value(self):
        return {"$toDate": self.input}

class ToObjectId(OpExpression):
    def __init__(self, input: Expression) -> None:
        self.input = input

    @property
    def repr_value(self):
        return {"$toObjectId": self.input}

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
