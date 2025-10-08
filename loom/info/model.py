from abc import ABC
import hashlib
import json
from typing import Optional, Type, TypeVar

from bson import ObjectId
from pydantic import BaseModel, ConfigDict, Field, PrivateAttr

from loom.info.field import InitializeValue, ModelFields, NormalizeValue, BeforeSetAttr, coalesce


ModelGen = TypeVar("ModelGen", bound="Model")


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

    id: ObjectId | None = Field(
        description="A universal id that this model entity can be linked with.",
        alias="_id",
        default=None,
    )

    _has_initialized: bool = PrivateAttr(False)
    _has_update: bool = PrivateAttr(default=True)
    _original_hash_from_doc: Optional[str] = PrivateAttr(default=None)

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        populate_by_name=True,
        protected_namespaces=(),
    )

    def __init__(self, **data):

        for field, transformers in self.get_fields_with_metadata(InitializeValue).items():
            value = data.get(field)
            new_value = coalesce(value, transformers)
            data[field] = new_value

        super().__init__(**data)

        for field, transformers in self.get_fields_with_metadata(NormalizeValue).items():
            self.coalesce_field(field, transformers)

        self._has_initialized = True

    @classmethod
    def fields(cls):
        return ModelFields(cls.model_fields)


    @property
    def has_update(self) -> bool:
        """
        Checks if the model has been updated since it was last persisted.

        Returns:
            bool: `True` if the model has pending changes, `False` otherwise.
        """

        if (self._has_update) :
            return True # _has_update variable will override hash comparison
        
        if self._original_hash_from_doc and self.hash_model() != self._original_hash_from_doc:
            return True
        
        return False

    def mark_updated(self):
        """
        Sets the model has been updated. (Forced to true)

        """
        self._has_update = True

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

    def coalesce_fields_for(self, field_type: type):
        for field, transformers in self.get_fields_with_metadata(field_type).items():
            self.coalesce_field(field, transformers)

    def collapse_id(self) -> ObjectId:
        """
        Gets or creates the `ObjectId` for the model instance.

        This method "collapses" the ID from its potential state to a definite one.

        Returns:
            ObjectId: The document's unique `ObjectId`.
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
        return self.id is not None and ObjectId.is_valid(str(self.id))

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
    def get_field_metadata(
        cls, field_name: str, hint_type: Optional[type] = None
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
        field_info = cls.model_fields.get(field_name)
        if field_info is None:
            return []

        metadata = field_info.metadata
        if hint_type is None:
            return metadata

        return [item for item in metadata if isinstance(item, hint_type)]

    @classmethod
    def from_doc(cls: Type[ModelGen], doc: dict):
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

    def custom_json_encoder(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj)
        
        return str(obj)

    def dump_json(self) -> str:
        """
        Serializes the model to a JSON string.

        Returns:
            str: The model as a JSON string.
        """

        # Purposefully sacrificing performance for consistency by using dump_doc. 
        # pydantic's model_dump_json, although more performance, has its set of issues
        # Also custom_json_encoder would give the child Model class more control

        model_doc = self.dump_doc()
        model_json = json.dumps(model_doc, sort_keys=True, default=self.custom_json_encoder)
        return model_json
    
    def hash_model(self) -> str:
        model_json = self.dump_json()
        model_bytes = model_json.encode('utf-8')

        hasher = hashlib.sha256()
        hasher.update(model_bytes)
        
        return hasher.hexdigest()

    def init_private_fields_from_doc(self, doc: dict) -> None:
        """
        A hook for initializing private fields from a database document.

        This method is called when creating a model instance via `from_db_doc`.
        Subclasses can override this to perform custom initialization, such as
        setting flags to indicate the model is persisted.

        Args:
            doc (dict): The document retrieved from the database.
        """
        self._has_update = False
        self._original_hash_from_doc = self.hash_model()

    def __setattr__(self, name: str, value) -> None:
        """
        Sets an attribute on the model, applying any `BeforeSetAttr` transformers.
        """
        if self._has_initialized:
            transformers = self.get_field_metadata(name, BeforeSetAttr)
            value = coalesce(value, transformers)

            transformers = self.get_field_metadata(name, NormalizeValue)
            value = coalesce(value, transformers)
        
        super().__setattr__(name, value)
