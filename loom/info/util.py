"""
Utility functions for the info module.
"""

from typing import get_origin, get_args, Union, Annotated

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


def get_base_type(annotation):
    """
    Iteratively unwraps Annotated and Optional/Union types.
    
    Handles:
    - Annotated[T, ...] -> T
    - Optional[T] -> T
    - Union[T, None] -> T
    - Union[None, T] -> T
    """
    while True:
        origin = get_origin(annotation)
        args = get_args(annotation)

        if origin is Annotated:
            annotation = args[0]
            continue
        
        if origin is Union:
            # Filter out NoneType
            non_none_args = [arg for arg in args if arg is not type(None)]
            
            # If exactly one type remains (e.g., Optional[int] -> int), unwrap it
            if len(non_none_args) == 1:
                annotation = non_none_args[0]
                continue
        
        # Stop if no known wrapper checks match
        return annotation
