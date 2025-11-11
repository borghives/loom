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
