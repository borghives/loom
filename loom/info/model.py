from abc import ABC
from datetime import datetime
from enum import Enum
from typing import Annotated, Optional

from bson import ObjectId
from pydantic import AfterValidator, BaseModel, ConfigDict, Field, PlainSerializer

from loom.time.util import get_current_time, to_utc_aware


class UpdateType(Enum):
    """
    Specifies the type of MongoDB update operation for a field.
    """

    SET_ON_INSERT = "$setOnInsert"
    SET = "$set"
    INC = "$inc"


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
    A `Collapsible` that finalize a value on document creation.

    If the field's value is `None`, it calls the `collapse` function to generate
    a new value. This is intended for use with `$setOnInsert` operations.
    """

    def __init__(self, collapse):
        self.collapse = collapse

    def __call__(self, v):
        if v is None:
            return self.collapse()
        return v


class CoalesceOnSet(Collapsible):
    """
    A `Collapsible` that provides a value on any update.

    If the field's value is `None`, it calls the `collapse` function to generate
    a new value. This is intended for use with `$set` operations where a value
    needs to be refreshed on every save.
    """

    def __init__(self, collapse):
        self.collapse = collapse

    def __call__(self, v):
        if v is None:
            return self.collapse()
        return v

class NormalizeQueryInput(Collapsible):
    """
    A `Collapsible` that provides a normalize value for query input.

    This is intended for use with query input where a value needs to be
    normalized based the field attribute.
    """

    def __init__(self, collapse):
        self.collapse = collapse

    def __call__(self, v):
        return self.collapse(v)


class CoalesceOnIncr(Collapsible):
    """
    A `Collapsible` that provides an increment value on update.

    If the field's value is `None`, it calls the `collapse` function to generate
    a new value. This is intended for use with `$inc` operations.
    """

    def __init__(self, collapse):
        self.collapse = collapse

    def __call__(self, v):
        if v is None:
            return self.collapse()
        return v


def coalesce(value, transformers: list):
    """Applies a list of transformers sequentially to a value."""
    for transformer in transformers:
        value = transformer(value)
    return value


class BeforeSetAttr:
    """
    An annotation to transform a value before it is set on a model field.

    This is used within the model's `__setattr__` to apply a function to the
    value before it is assigned to the attribute.
    """
    def __init__(self, func):
        self.func = func

    def __call__(self, v):
        return self.func(v)


class AfterPersist:
    """
    An annotation for logic to be executed after a model has been persisted.

    This allows for custom logic to be executed after the model has been saved to
    the database, such as updating internal state or triggering side effects.
    This is not a Pydantic validator, but a marker for other parts of the
    persistence logic.
    """

    def __init__(self, func):
        self.func = func

    def __call__(self, v):
        """
        Executes the wrapped function.

        Args:
            v: The value of the field.

        Returns:
            The result of the wrapped function.
        """
        return self.func(v)


#: An annotated string type that automatically converts the value to uppercase.
StrUpper = Annotated[
    str, 
    AfterValidator(str.upper), 
    CoalesceOnSet(str.upper),
    NormalizeQueryInput(str.upper)
]

#: An annotated string type that automatically converts the value to lowercase.
StrLower = Annotated[
    str, 
    AfterValidator(str.lower), 
    CoalesceOnSet(str.lower),
    NormalizeQueryInput(str.lower),
]

#: A datetime field that defaults to the current UTC time on document creation.
TimeInserted = Annotated[
    datetime | None,
    AfterValidator(lambda x: to_utc_aware(x) if x is not None else None),
    CoalesceOnInsert(collapse=get_current_time),
]

#: A datetime field that defaults to the current UTC time on document update.
TimeUpdated = Annotated[
    datetime | None,
    AfterValidator(lambda x: to_utc_aware(x) if x is not None else None),
    CoalesceOnSet(collapse=get_current_time),
]


JsObjectId = Annotated[ObjectId, PlainSerializer(str, when_used='json')] # A serializer for ObjectId to convert it to a string in JSON output.
JsSet = Annotated[set, PlainSerializer(list, when_used='json')] # A serializer for set to convert it to a list in JSON output.

class Model(ABC, BaseModel):
    """
    An abstract base class for creating Pydantic models for the application.

    This class provides core functionality for integrating with MongoDB, including:
    - Automatic generation of an `ObjectId` for the `id` field on creation.
    - Serialization to and from MongoDB-compatible documents.
    - Helper methods for inspecting field metadata and annotations.

    Attributes:
        id (ObjectId): The document's unique identifier, aliased to `_id` for
            MongoDB compatibility. It automatically generates a new `ObjectId`
            on first access if one is not already present.
    """

    id: JsObjectId | None = Field(
        description="A universal id that this model entity can be linked with.",
        alias="_id",
        default=None,
    )

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        populate_by_name=True,
        protected_namespaces=(),
    )

    def coalesce_field(self, field_name: str, transformers: list):
        """
        Coalesces a field's value using the provided transformers.

        This method retrieves the current value of the specified field and applies
        the list of transformers to it sequentially. If the field's value is `None`,
        it will be transformed by the first transformer that can handle `None`.

        Args:
            field_name (str): The name of the field to coalesce.
            transformers (list): A list of transformer functions to apply.

        Returns:
            Any: The coalesced value for the field.
        """

        current_value = getattr(self, field_name)
        new_value = coalesce(current_value, transformers)
        setattr(self, field_name, new_value)
        return new_value

    def collapse_id(self) -> ObjectId:
        """
        Gets or creates the `ObjectId` for the model instance.

        This method "collapses" the ID from its potential state to a definite one.
        If the `id` field is `None`, it triggers the `CoalesceOnInsert` logic
        from the `SuperId` annotation to generate a new `ObjectId`.

        Returns:
            ObjectId: The document's unique `ObjectId`.

        Raises:
            ValueError: If the collapsing logic fails to produce an `ObjectId`.
        """
        if self.id is not None:
            return self.id

        self.id = ObjectId()
        return self.id

    def is_entangled(self) -> bool:
        """
        Checks if the model has been assigned a persistent identity.

        Once an `ObjectId` is present, the model is considered "entangled" or
        linked to a potential database record.

        Returns:
            bool: `True` if the document has an `ObjectId`, `False` otherwise.
        """
        return self.id is not None

    @classmethod
    def get_fields_with_metadata(cls, meta_type) -> dict[str, list]:
        """
        Finds all model fields that are annotated with a specific metadata type.

        This is useful for discovering fields with custom metadata annotations like
        `CoalesceOnInsert` or `QueryableTransformer`.

        Args:
            meta_type: The type of the metadata to look for.

        Returns:
            A dictionary where keys are field names and values are lists of
            matching metadata items.
        """
        meta_map = {
            key: [item for item in value.metadata if isinstance(item, meta_type)]
            for key, value in cls.model_fields.items()
        }

        return {key: value for key, value in meta_map.items() if len(value) > 0}

    @classmethod
    def get_field_metadata(cls, field_name: str, hint_type: Optional[type] = None) -> list:
        """
        Gets the metadata for a specific field, optionally filtered by type.

        Args:
            field_name: The name of the field to inspect.
            hint_type (type, optional): If provided, only metadata items of this type are
                returned. Defaults to None.

        Returns:
            A list of metadata items found on the field.
        """
        annotation_info = cls.model_fields.get(field_name)
        if annotation_info is None:
            return []

        metadata = getattr(annotation_info, "metadata", [])
        if hint_type is None:
            return metadata

        return [item for item in metadata if isinstance(item, hint_type)]

    def dump_doc(self) -> dict:
        """
        Serializes the model to a MongoDB-compatible dictionary.

        It uses `by_alias=True` to ensure field aliases (like `_id`) are used
        and excludes fields with `None` values.

        Returns:
            dict: The model as a dictionary.
        """
        retval = self.model_dump(by_alias=True, exclude_none=True)
        return retval

    def dump_json(self) -> str:
        """
        Serializes the model to a JSON string.

        Returns:
            str: The model as a JSON string.
        """
        return self.model_dump_json(by_alias=True, exclude_none=True)

    def init_private_fields_from_doc(self, doc):
        """
        A hook for initializing private fields from a database document.

        This method is called when creating a model instance via `from_db_doc`.
        Subclasses can override this to perform custom initialization, such as
        setting flags to indicate the model is persisted.

        Args:
            doc (dict): The document retrieved from the database.
        """
        pass

    def __setattr__(self, name: str, value) -> None:
        """
        Sets an attribute on the model, applying any `BeforeSetAttr` transformers.
        """
        transformers = self.get_field_metadata(name, BeforeSetAttr)
        value = coalesce(value, transformers)
        return super().__setattr__(name, value)

    @classmethod
    def from_db_doc(cls, doc: dict):
        """
        Creates a model instance from a MongoDB document.

        After creating the instance, it calls the `init_private_fields_from_doc`
        hook to allow for post-initialization logic.

        Args:
            doc (dict): The MongoDB document.

        Returns:
            An instance of the model populated with the document data.
        """
        retval = cls(**doc)
        retval.init_private_fields_from_doc(doc)
        return retval
