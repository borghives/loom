"""
Utility functions for the info module.
"""

def coalesce(value, transformers: list):
    """
    Applies a list of transformers sequentially to a value.

    Args:
        value: The initial value.
        transformers (list): A list of callable transformers.

    Returns:
        The transformed value.
    """
    for transformer in transformers:
        value = transformer(value)
    return value
