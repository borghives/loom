from datetime import datetime
from enum import Enum
from functools import wraps
from typing import Annotated, Optional

from pydantic import AfterValidator, Field
from pydantic.fields import FieldInfo
from loom.info.expression import Expression
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
from loom.time.util import get_current_time, to_utc_aware

def suppress_warning(func):
    """suppress pylance error :-/ Not proud of myself! Picking on a dumb kid who you like"""
    @wraps(func) 
    def wrapper(self, other):
        return func(self, other)
    return wrapper

class QueryableField:
    def __init__(self, name: str, field_info: Optional[FieldInfo] = None):
        self.name = name
        self.field_info = field_info

    def get_query_name(self) -> str:
        if self.field_info is None:
            return self.name
        
        return self.field_info.alias or self.name

    def get_field_metadata(
        self, hint_type: Optional[type] = None
    ) -> list:
        """
        Gets the metadata for a specific field, optionally filtered by type.

        Args:
            field_name: The name of the field to inspect.
            hint_type (type, optional): If provided, only metadata items of this type are
                returned. Defaults to None.

        Returns:
            A list of metadata items found on the field.
        """
        if self.field_info is None:
            return []

        metadata = self.field_info.metadata
        if hint_type is None:
            return metadata

        return [item for item in metadata if isinstance(item, hint_type)]
    
    def normalize_query_input(self, value):
        transformers = self.get_field_metadata(NormalizeQueryInput)
        if (transformers is None or len(transformers) == 0):
            return value
        
        if isinstance(value, dict):
            return {k: self.normalize_query_input(v) for k, v in value.items()}
        
        if isinstance(value, list):
            return [self.normalize_query_input(v) for v in value]
        
        return coalesce(value, transformers)

    def __gt__(self, other) -> Filter:
        other=self.normalize_query_input(other)
        return self.predicate(Gt(other))

    def __lt__(self, other) -> Filter:
        other=self.normalize_query_input(other)
        return self.predicate(Lt(other))

    def __ge__(self, other) -> Filter:
        other=self.normalize_query_input(other)
        return self.predicate(Gte(other)) 

    def __le__(self, other) -> Filter:
        other=self.normalize_query_input(other)
        return self.predicate(Lte(other)) 

    @suppress_warning
    def __eq__(self, other) -> Filter:
        if isinstance(other, Enum):
            return self.is_enum(other)
        
        if isinstance(other, Expression):
            return self.predicate(Eq(other))
        
        other=self.normalize_query_input(other)
        return Filter({self.get_query_name(): other})

    @suppress_warning
    def __ne__(self, other) -> Filter:
        other=self.normalize_query_input(other)
        return self.predicate(Ne(other)) 

    def is_in(self, other) -> Filter:
        other=self.normalize_query_input(other)
        return self.predicate(In(other)) 
    
    def is_not_in(self, other) -> Filter:
        other=self.normalize_query_input(other)
        return self.predicate(NotIn(other))
    
    def is_all(self, other) -> Filter:
        other=self.normalize_query_input(other)
        return self.predicate(All(other))
    
    def is_not_all(self, other) -> Filter:
        other=self.normalize_query_input(other)
        return self.predicate(NotAll(other))
    
    def is_within(self, other: Time | TimeFrame):
        if isinstance(other, TimeFrame):
            return self.predicate(Time().in_frame(other))
        
        return self.predicate(other)
    
    def is_enum(self, other: Enum) -> Filter:
        if (other.value == "ANY"):
            return Filter()
        return Filter({self.get_query_name(): other.value})
    
    def is_false(self) -> Filter:
        return Filter({self.get_query_name(): False})
    
    def is_true(self) -> Filter:
        return Filter({self.get_query_name(): True})
    
    def is_exists(self) -> Filter:
        return self.predicate(Exists(True))
    
    def is_not_exists(self) -> Filter:
        return self.predicate(Exists(False))
    
    def is_none_or_missing(self) -> Filter:
        return Filter({self.get_query_name(): None})
    
    def predicate(self, query_op: QueryOpExpression) -> Filter:
        return Filter({self.get_query_name(): query_op})


class Collapsible:
    """
    Abstract base for annotations that generate a value on demand.

    This pattern is used for fields that get a final form of their value when they are
    explicitly "collapsed".  The __call__ function should be idempotent as in f(f(x)) == f(x).
    """

    def __call__(self, v):
        raise NotImplementedError()


class CoalesceOnInsert(Collapsible):
    """
    A Collapsible that finalize a value on document creation for database insertion (not update).
    """

    def __init__(self, collapse):
        self.collapse = collapse

    def __call__(self, v):
        if v is None:
            return self.collapse()
        return v


class CoalesceOnIncr(Collapsible):
    """
    A Collapsible that provides an increment value on update.

    If the field's value is `None`, it calls the `collapse` function to generate
    a new value. This is intended for use with `$inc` operations.
    """

    def __init__(self, collapse):
        self.collapse = collapse

    def __call__(self, v):
        if v is None:
            return self.collapse()
        return v

class Refreshable:
    """
    Abstract base for annotations that refresh a value on demand.

    This pattern is used for fields that get a final form of their value when they are
    explicitly "refreshed".  The __call__ function should be idempotent as in f(f(x)) == f(x).
    """
    def __init__(self, refresh):
        self.refresh = refresh

    def __call__(self, v):
        return self.refresh(v)

class RefreshOnSet(Refreshable):
    """
    A Refreshable that provides a value on any database update (not insertion).
    This is intended when a value needs to be refreshed on every save.
    """


class NormalizeValue(Refreshable):
    """
    A Refreshable that provides a normalize value for query input.

    This is intended for use with query input where a value needs to be
    normalized based the field attribute.
    """

class NormalizeQueryInput(Refreshable):
    """
    A Refreshable that provides a normalize value for query input.

    This is intended for use with query input where a value needs to be
    normalized based the field attribute.
    """

class BeforeSetAttr(Refreshable):
    """
    An annotation to transform a value before it is set on a model field.
    This only applies when the model has been fully initialized.

    This is used within the model's `__setattr__` to apply a function to the
    value before it is assigned to the attribute.
    """

class InitializeValue(Refreshable):
    """
    An annotation to transform a value before it is initialized.

    This is used within the model's `__init__` to apply a function to the value
    """

def coalesce(value, transformers: list):
    """Applies a list of transformers sequentially to a value."""
    for transformer in transformers:
        value = transformer(value)
    return value

#: An annotated string type that automatically converts the value to uppercase.
StrUpper = Annotated[
    str,
    NormalizeValue(str.upper),
    NormalizeQueryInput(str.upper),
]

#: An annotated string type that automatically converts the value to lowercase.
StrLower = Annotated[
    str,
    NormalizeValue(str.lower),
    NormalizeQueryInput(str.lower),
]

#: A datetime field that defaults to the current UTC time on document creation.
TimeInserted = Annotated[
    datetime | None,
    AfterValidator(lambda x: to_utc_aware(x) if x is not None else None),
    CoalesceOnInsert(get_current_time),
    Field(default=None),
]

#: A datetime field that defaults to the current UTC time on document update.
TimeUpdated = Annotated[
    datetime | None,
    AfterValidator(lambda x: to_utc_aware(x) if x is not None else None),
    RefreshOnSet(lambda x: get_current_time()),
    Field(default=None),
]

TimeNorm = Annotated[
    datetime,
    NormalizeValue(to_utc_aware),
    NormalizeQueryInput(to_utc_aware),
]

class ModelFields:
    def __init__(self, fields: dict[str, FieldInfo]):
        self.fields = fields
    
    def __getitem__(self, key) -> QueryableField:
        return QueryableField(key, self.fields[key])
    
    def __len__(self):
        return len(self.fields)
    
    def __contains__(self, key):
        return key in self.fields
    