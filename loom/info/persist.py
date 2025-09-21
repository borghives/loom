from typing import Any, Optional, Tuple

from bson import ObjectId
import pandas as pd
from pydantic import Field, PrivateAttr
from pymongo import MongoClient, ReturnDocument, UpdateOne
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import BulkWriteError

from pymongoarrow.api import Schema #type: ignore
from loom.info.field import fld
from loom.info.filter import Filter
from loom.info.aggregation import Aggregation
from loom.info.model import (
    CoalesceOnIncr,
    CoalesceOnInsert,
    Model,
    RefreshOnDataframeInsert,
    RefreshOnSet,
    TimeInserted,
    TimeUpdated,
    coalesce,
)
from loom.info.universal import get_local_db_client, get_remote_db_client

class Persistable(Model):
    """
    A mixin for Pydantic models providing MongoDB persistence capabilities.

    This class extends `loom.info.model.Model` to offer a suite of methods for
    database interactions, including creating, reading, and querying documents.
    It requires a decorator like `@declare_persist_db` to be applied to the
    subclass to define the database and collection metadata.

    It also supports automatic timestamp management for creation and updates
    and provides helpers for constructing atomic update operations.

    Attributes:
        updated_time (TimeUpdated): A timestamp that is automatically updated
            when the model is persisted to database.
        created_at (TimeInserted): A timestamp that is set once when the model
            is first inserted into the database.
        _has_update (bool): A private flag indicating if the model has pending
            changes to be persisted.  It is the responsibility of the logic to
            update this flag.  Defaults to `True` on creation.
    """

    updated_time: TimeUpdated = Field(
        description="Timestamp of the last update.", default=None
    )
    
    created_at: TimeInserted = Field(
        description="Entity Created Time (does not exist if entity has not been persisted)",
        default=None,
    )

    # newly constructed model needs persisting.  model from store should go through from_db_doc() which set _has_update to False
    _has_update: bool = PrivateAttr(default=True)

    @property
    def has_update(self) -> bool:
        """
        Checks if the model has been updated since it was last persisted.

        Returns:
            bool: `True` if the model has pending changes, `False` otherwise.
        """

        return self._has_update

    @has_update.setter
    def has_update(self, value: bool):
        """
        Sets the update status of the model.

        Args:
            value (bool): The new update status.
        """
        self._has_update = value

    @property
    def should_persist(self) -> bool:
        """
        Checks if the model should be persisted. Mainly used by lazy persist

        Returns:
            bool: `True` if the model should be persisted, `False` otherwise.
        """
        return self.has_update or not self.is_entangled()

    def self_filter(self) -> dict:
        """
        Returns a filter to find the current document in the database by its ID.

        Returns:
            dict: A filter expression for finding the document by its `_id`.
        """
        return {"_id": self.collapse_id()}

    def get_set_on_insert_instruction(self) -> Tuple[dict, list]:
        """
        Constructs the `$setOnInsert` part of a MongoDB update operation.

        This includes the model version and any fields marked with
        `CoalesceOnInsert` that should only be set when a document is first
        created.

        Returns:
            Tuple[dict, list]: A tuple containing the `$setOnInsert` dictionary
            and a list of the fields included in the operation.
        """
        model_version = self.get_model_code_version()
        set_on_insert_op: dict = {}
        if model_version is not None:
            set_on_insert_op["version"] = model_version

        for field, transformers in self.get_fields_with_metadata(
            CoalesceOnInsert
        ).items():
            set_on_insert_op[field] = self.coalesce_field(field, transformers)

        if len(set_on_insert_op) == 0:
            return {}, []

        return {"$setOnInsert": set_on_insert_op}, list(set_on_insert_op.keys())

    def get_increment_instruction(self) -> Tuple[dict, list]:
        """
        Constructs the `$inc` part of a MongoDB update operation.

        It processes fields marked with `CoalesceOnIncr`. It is expected that
        these fields have a `get_changes()` method that returns the value to
        increment by.

        Returns:
            Tuple[dict, list]: A tuple containing the `$inc` dictionary and a
            list of the fields included in the operation.
        """
        increment_instruction: dict = {}
        for field, transformers in self.get_fields_with_metadata(
            CoalesceOnIncr
        ).items():
            increment_value = getattr(self, field).get_changes()
            increment_instruction[field] = increment_value
            self.coalesce_field(field, transformers)

        if len(increment_instruction) == 0:
            return {}, []

        return {"$inc": increment_instruction}, list(increment_instruction.keys())

    def get_set_instruction(self, exclude_fields: list = []) -> Tuple[dict, list]:
        """
        Constructs the `$set` part of a MongoDB update operation.

        This includes all model fields except those specified in `exclude_fields`.
        It also processes fields marked with `CoalesceOnSet`.

        Args:
            exclude_fields (list, optional): A list of fields to exclude from
                the `$set` operation. Defaults to [].

        Returns:
            Tuple[dict, list]: A tuple containing the `$set` dictionary and a
            list of the fields included in the operation.
        """
        doc = self.dump_doc()
        
        for field, transformers in self.get_fields_with_metadata(RefreshOnSet).items():
            doc[field] = self.coalesce_field(field, transformers)

        for field in exclude_fields:
            if field in doc:
                del doc[field]

        if len(doc) == 0:
            return {}, []

        return {"$set": doc}, list(doc.keys())

    def get_update_instruction(self) -> dict[str, Any]:
        """
        Constructs a complete MongoDB update instruction for an upsert operation.

        This method separates fields into a `$set` operation for updates and a
        `$setOnInsert` operation for fields that should only be written on
        document creation.

        Returns:
            dict: A dictionary representing the full update instruction.
        """

        set_on_insert_instruction, set_insert_fields = (
            self.get_set_on_insert_instruction()
        )
        increment_instruction, inc_fields = self.get_increment_instruction()
        set_instruction, _ = self.get_set_instruction(
            exclude_fields=inc_fields + set_insert_fields
        )
        update_instr: dict[str, Any] = {}

        update_instr.update(set_instruction)
        update_instr.update(set_on_insert_instruction)
        update_instr.update(increment_instruction)

        return update_instr

    @classmethod
    def from_db_doc(cls, doc):
        retval = super().from_db_doc(doc)
        retval._has_update = False  # initial load from doc
        return retval

    # --- Information on how to persist the model ---
    @classmethod
    def get_db_info(cls) -> dict:
        """
        Gets the database metadata dictionary for the model.

        This metadata is expected to be set by a `@declare_persist_db` decorator
        on the model class.

        Returns:
            dict: The database information for the model.

        Raises:
            Exception: If the class is not decorated with `@declare_persist_db`.
        """
        if not hasattr(cls, "_db_metadata"):
            raise Exception(
                f"Class {cls.__name__} is not decorated with @declare_persist_db"
            )

        return getattr(cls, "_db_metadata")

    @classmethod
    def get_db_collection_name(cls) -> str:
        """
        Gets the name of the MongoDB collection for the model.

        Returns:
            str: The name of the MongoDB collection.

        Raises:
            ValueError: If `collection_name` is not defined in the metadata.
        """
        db_info = cls.get_db_info()
        name = db_info.get("collection_name")
        if not name:
            raise ValueError(
                f"Class {cls.__name__} does not have collection_name defined in @declare_persist_db"
            )
        return name

    @classmethod
    def get_db_name(cls) -> str:
        """
        Gets the name of the MongoDB database for the model.

        Returns:
            str: The name of the MongoDB database.

        Raises:
            ValueError: If `db_name` is not defined in the metadata.
        """
        db_info = cls.get_db_info()
        name = db_info.get("db_name")
        if not name:
            raise ValueError(
                f"Class {cls.__name__} does not have db_name defined in @declare_persist_db"
            )
        return name

    @classmethod
    def get_db_client(cls) -> MongoClient:
        """
        Gets the appropriate MongoDB client for the model (local or remote).

        Returns:
            MongoClient: The MongoDB client instance.
        """
        if cls.is_remote_db():
            return get_remote_db_client()
        else:
            return get_local_db_client()

    @classmethod
    def is_remote_db(cls) -> bool:
        """
        Checks if the model is configured to use a remote database.

        Returns:
            bool: `True` if the model uses a remote database, `False` otherwise.
        """
        db_info = cls.get_db_info()
        return db_info.get("remote_db", False)

    @classmethod
    def get_db(cls) -> Database:
        """
        Gets the MongoDB database object for the model.

        Returns:
            Database: The `pymongo.database.Database` instance.
        """
        client = cls.get_db_client()
        db_name = cls.get_db_name()
        return client[db_name]

    @classmethod
    def get_db_collection(cls) -> Collection:
        """
        Gets the MongoDB collection object for the model.

        Returns:
            Collection: The `pymongo.collection.Collection` instance.
        """
        client = cls.get_db_client()
        db_name = cls.get_db_name()
        collection_name = cls.get_db_collection_name()
        return client[db_name][collection_name]

    @classmethod
    def get_model_code_version(cls) -> Optional[int]:
        """
        Gets the version of the model schema, if defined in the metadata.

        Returns:
            Optional[int]: The version number of the model.
        """
        db_info = cls.get_db_info()
        return db_info.get("version")

    # ---END: Information on how to persist the model ---

    # --- Save to persistence storage ---
    def on_after_persist(self, result_doc: Optional[Any] = None):
        self.has_update = False
        if result_doc:
            # Update the current object with the values from the database
            self.__dict__.update(self.from_db_doc(result_doc).__dict__)

    def persist(self, lazy: bool = False) -> bool:
        """
        Saves the model to the database.

        If the model does not have an `_id`, a new document will be created.
        Otherwise, the existing document will be updated.
        """

        if lazy and not self.should_persist:
            return False

        collection = self.get_db_collection()

        filter = self.self_filter()
        update = self.get_update_instruction()

        result = collection.find_one_and_update(
            filter,
            update,
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )

        self.on_after_persist(result)

        return True

    @classmethod
    def persist_many(cls, items: list, lazy: bool = False):
        """
        Saves multiple models to the database.

        Args:
            items (list): A list of models to save.
        """
        operations: list = []

        persist_items = [
            item
            for item in items
            if isinstance(item, Persistable) and (item.should_persist or not lazy)
        ]

        for item in persist_items:
            update_op = UpdateOne(
                item.self_filter(),
                item.get_update_instruction(),
                upsert=True,
            )
            operations.append(update_op)

        collection = cls.get_db_collection()
        collection.bulk_write(operations)

        for item in persist_items:
            item.on_after_persist()

    @classmethod
    def insert_dataframe(
        cls, dataframe: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Inserts a pandas DataFrame into the database.

        Note: to have RefreshOnDataframeInsert fields refresh, the columns must exist in the DataFrame.  
        Default columns to None if to have field trigger content.

        Args:
            dataframe (pd.DataFrame): The DataFrame to insert.
        """
        if dataframe.empty:
            return dataframe

        collection = cls.get_db_collection()

        transformer_map = cls.get_fields_with_metadata(RefreshOnDataframeInsert)
        for key, transformers in transformer_map.items():
            if key in dataframe.columns:
                dataframe[key] = dataframe[key].apply(lambda x: coalesce(x, transformers))

        model_version = cls.get_model_code_version()
        if model_version is not None:
            dataframe["version"] = model_version

        try:
            collection.insert_many(
                dataframe.to_dict("records"), ordered=False
            )  # ordered false so that a duplicate key error won't stop the insert of many
        except BulkWriteError as bwe:
            # If there are errors other than duplicate key (11000), re-raise the original exception.
            # This preserves the full error context for the caller to handle.
            if any(error['code'] != 11000 for error in bwe.details['writeErrors']):
                raise
                
        return dataframe

    # ---END: Save to persistence storage ---

    @classmethod
    def from_id(cls, id: ObjectId | str):
        """
        Loads a document from the database by its `ObjectId`.

        Args:
            id (ObjectId | str): The `ObjectId` of the document, either as an
                `ObjectId` instance or its string representation.

        Returns:
            Optional[Self]: An instance of the loaded document, or `None` if not
                found or the ID is invalid.
        """
        if isinstance(id, str):
            if not ObjectId.is_valid(id):
                return None
            id = ObjectId(id)
        return cls.filter(fld("_id") == id).load_one()
    
    @classmethod
    def filter(cls, filter: Filter = Filter()):
        from loom.info.directive import LoadDirective
        return LoadDirective(cls).filter(filter)

    @classmethod
    def aggregation(cls, aggregation: Aggregation = Aggregation()):
        from loom.info.directive import LoadDirective
        return LoadDirective(cls).aggregation(aggregation)

    # --- PyArrow ---
    @classmethod
    def get_arrow_schema(cls) -> Optional[Schema]:
        """
        Generates a PyMongoArrow Schema from the Pydantic model fields.
        Returns an explicit schema for model
        """
        return None
    # --- END: PyArrow ---

    @classmethod
    def create_collection(cls):
        """
        Creates the MongoDB collection for the model if it does not exist.
        """
        db = cls.get_db()
        collection_names = db.list_collection_names()
        name = cls.get_db_collection_name()

        if name not in collection_names:
            db.create_collection(
                name,
            )


def declare_persist_db(
    collection_name: str,
    db_name: str,
    remote_db: bool = False,
    version: Optional[int] = None,
    test: bool = False,
):
    """
    A class decorator that declares how a model should be persisted to
    MongoDB.

    Args:
        collection_name (str): The name of the MongoDB collection.
        db_name (str): The name of the MongoDB database.
        remote_db (bool, optional): Whether the database is remote. Defaults
            to `False`.
        version (Optional[int], optional): The version of the model. Defaults
            to `None`.
        test (bool, optional): Whether the model is a test model. Defaults
            to `False`.
    """

    def decorator(cls):
        final_collection_name = f"{collection_name}_test" if test else collection_name

        cls._db_metadata = {
            "collection_name": final_collection_name,
            "db_name": db_name,
            "remote_db": remote_db,
            "version": version,
        }
        return cls

    return decorator
