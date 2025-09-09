import json
from abc import ABC
from datetime import datetime
from enum import Enum
from typing import Annotated, Optional

from bson import ObjectId
from pydantic import AfterValidator, BaseModel, ConfigDict, Field


class QueryableTransformer(AfterValidator):
    """
    An annotation for Pydantic models that applies a transformation function.
    This is used for both validation and for transforming query values.
    """

    def __call__(self, v):
        # This allows the instance to be used as a function,
        # which is what AfterValidator expects.
        return self.func(v)  # type: ignore


class UpdateType(Enum):
    """
    Specifies the type of update operation for a field when persisting.
    """

    SET_ON_INSERT = "$setOnInsert"
    SET = "$set"
    INC = "$inc"


class Superposition:
    """
    A Pydantic annotation for fields that are in a "superposition" of existence.
    It value is not known until entangled with or observed by other where it collapse to a real object

    This allows a field to have a default value that collapses to a real object upon creation,
    and specifies how the field should be updated in the database.
    """

    def __init__(self, collapse, collapse_on: UpdateType):
        """
        Initializes the Superposition annotation.

        Args:
            collapse: A function to call to get the default value if the field is None.
            collapse_on: The database update operation to use for this field.
        """
        self.collapse = collapse
        self.collapse_on = collapse_on

    def __call__(self, v):
        if v is None:
            return self.collapse()
        return v


def coalesce(value, transformers: list):
    for transformer in transformers:
        value = transformer(value)
    return value


StrUpper = Annotated[str, QueryableTransformer(str.upper)]
StrLower = Annotated[str, QueryableTransformer(str.lower)]

# A datetime field that defaults to the current time on insert.
SuperposDate = Annotated[
    datetime | None,
    Superposition(collapse=datetime.now, collapse_on=UpdateType.SET_ON_INSERT),
]
# An ObjectId field that defaults to a new ObjectId on insert.
SuperposId = Annotated[
    ObjectId | None,
    Superposition(collapse=ObjectId, collapse_on=UpdateType.SET_ON_INSERT),
]


class Model(ABC, BaseModel):
    """
    The Model is the abstract base class for creating Pydantic models that can be woven into the Loom.
    It can be thought of as a Fiber, a thread of information that can be persisted and entangled with other Fibers.

    This class provides the basic functionality for handling MongoDB's `ObjectId`
    and for serializing the model to and from MongoDB documents.

    Attributes:
        id (SuperposId): The document's ID in MongoDB, aliased to `_id`.
            It's a "superposition" ID that collapses to a real ObjectId upon observation (access).
    """

    id: SuperposId = Field(
        description="A universal id that this model entity can be linked with.",
        alias="_id",
        default=None,
    )

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        populate_by_name=True,
        json_encoders={
            ObjectId: str,  # Encode ObjectId as string in JSON
            set: lambda s: list(s),  # Convert set to list in JSON
        },
        protected_namespaces=(),
    )

    def collapse_id(self) -> ObjectId:
        """
        Gets or Create the `ObjectId` for the model.  Like quantum decoherence, where the model superposition collapsed upon observation

        If the document does not have an `ObjectId` yet, a new one will be generated for entanglement.

        Returns:
            ObjectId: The document's `ObjectId`.
        """
        if self.id is None:
            superpositions = self.get_field_hint("id", Superposition)
            # The value is not set, so we collapse it from the superposition.
            value = coalesce(self.id, superpositions)
            if isinstance(value, ObjectId):
                self.id = value
                return value

            raise ValueError(
                f"collapsing of id did not return ObjectId type.  instead got {type(value)}"
            )

        return self.id

    def is_entangled(self) -> bool:
        """
        Checks if the document has an `ObjectId`.  Once its id exists, assumed that it was observed and entangled with other model.

        Returns:
            bool: `True` if the document has an `ObjectId`, `False` otherwise.
        """
        return self.id is not None

    @classmethod
    def get_field_hints(cls, meta_type) -> dict[str, list]:
        """
        Gets all fields that have a metadata item of a specific type.

        This is useful for finding fields with custom annotations like `Entanglement`.

        Args:
            meta_type: The type of the metadata to look for.

        Returns:
            A dictionary where keys are field names and values are lists of
            metadata items of the specified type.
        """
        meta_map = {
            key: [item for item in value.metadata if isinstance(item, meta_type)]
            for key, value in cls.model_fields.items()
        }

        return {key: value for key, value in meta_map.items() if len(value) > 0}

    @classmethod
    def get_field_hint(cls, field_name: str, hint_type: Optional[type] = None) -> list:
        """
        Gets the metadata for a specific field, optionally filtered by type.

        Args:
            field_name (str): The name of the field.
            hint_type (Optional[type], optional): The type of metadata to filter by.
                If None, all metadata for the field is returned. Defaults to None.

        Returns:
            A list of metadata items for the field.
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
        Dumps the model to a dictionary that is ready to be inserted into
        MongoDB.

        Returns:
            dict: The model as a MongoDB document.
        """
        retval = self.model_dump(by_alias=True, exclude_none=True)
        return retval

    def dump_json(self) -> str:
        """
        Dumps the model to a JSON string.

        Returns:
            str: The model as a JSON string.
        """
        doc = self.dump_doc()
        return json.dumps(doc)

    def initialize_private_fields(self, doc):
        """
        A hook for initializing fields from a database document.

        This method is called when loading a model from the database. Subclasses
        can override this to perform custom initialization.

        Args:
            doc (dict): The document retrieved from the database.
        """
        pass

    @classmethod
    def from_db_doc(cls, doc: dict) -> "Model":
        """
        Creates a new instance of the model from a MongoDB document.

        Args:
            doc (dict): The MongoDB document.

        Returns:
            Model: A new instance of the model.
        """
        retval = cls(**doc)
        retval.initialize_private_fields(doc)
        return retval
