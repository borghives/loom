from loom.info.db_driver import MongoDbModelDriver
from loom.info.db_client import LocalClientFactory
from loom.info.db_client import DbClientFactory
from loom.info.persist_operation import InsertOneOperation, UpdateOneOperation, PersistOperation
from pymongo.errors import DuplicateKeyError
from typing import Self
import asyncio
from typing import Any, ClassVar, List, Optional, Tuple, Type
from loom.info.directive import LoadDirective
import pandas as pd
import polars as pl

from bson import ObjectId
import pyarrow
from pydantic import Field

from pymongo import ReturnDocument, UpdateMany
from pymongo.collection import Collection
from pymongo.asynchronous.collection import AsyncCollection
from pymongo.errors import BulkWriteError

from loom.info.expression import ExpressionDriver
from loom.info.field import QueryableField
from loom.info.model import (
    CoalesceOnIncr,
    CoalesceOnInsert,
    RefreshOnSet,
    TimeInserted,
    TimeUpdated,
)
from loom.info.filter import QueryPredicates
from loom.info.aggregation import AggregationStages
from loom.info.index import Index
from loom.info.model import Model

class PersistableBase(Model):
    """
    A mixin for Pydantic models providing MongoDB persistence capabilities.

    This class extends `Model` to offer a suite of methods for
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
    """

    # --- Class State and Helpers ---
    _has_class_initialized : ClassVar[bool] = False
    _init_lock : ClassVar[asyncio.Lock] = asyncio.Lock()

    @classmethod
    def get_model_driver(cls) -> MongoDbModelDriver:
        retval = getattr(cls, "_db_model_driver", None)
        assert(retval is not None)
        return retval

    @classmethod
    def initialize_model(cls):
        if cls._has_class_initialized:
            return

        driver = cls.get_model_driver()
        driver.create_collection()
        driver.create_index()
        cls._has_class_initialized = True

    @classmethod
    async def initialize_model_async(cls):
        if cls._has_class_initialized:
            return
        
        async with cls._init_lock:
            if cls._has_class_initialized:
                return
            
            driver = cls.get_model_driver()
            await driver.create_collection_async()
            await driver.create_index_async()
            cls._has_class_initialized = True
    
    @classmethod
    def get_init_collection(cls) -> Collection:
        cls.initialize_model()
        retval = cls.get_model_driver().get_db_collection(with_async=False)
        assert(isinstance(retval, Collection))
        return retval

    @classmethod
    async def get_init_collection_async(cls) -> AsyncCollection:
        await cls.initialize_model_async()
        retval = cls.get_model_driver().get_db_collection(with_async=True)
        assert(isinstance(retval, AsyncCollection))
        return retval

    # --- Instance State and Helpers ---
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
        Returns a filter to find the current document.  This filter MUST be unique to the document.
        Defaults to using the `_id` field. Can be overridden to use other fields.

        Returns:
            dict: A filter expression for finding the document.
        """
        if self.is_entangled():
            return {"_id": self.id}
        else:
            return {}

    def on_after_persist(self, result_doc: dict):
        self._has_update = False
        # Update the current object with the values from the database
        self.__dict__.update(self.from_doc(result_doc).__dict__)
        self._original_hash_from_doc = self.hash_model()


    # --- Update Instruction Builders ---
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
        set_on_insert_op: dict = {}

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
    def convert_dataframe_to_records(cls, dataframe: pd.DataFrame | pl.DataFrame | pyarrow.Table) -> Optional[list]:
        records : Optional[list[dict]] = None

        if isinstance(dataframe, pd.DataFrame) and not dataframe.empty:
            records = dataframe.to_dict("records")
        elif isinstance(dataframe, pl.DataFrame) and not dataframe.is_empty():
            records = dataframe.to_dicts()
        elif isinstance(dataframe, pyarrow.Table) and dataframe.num_rows > 0:
            records = dataframe.to_pylist()

        if records is None or len(records) == 0:
            return None

        id_fields = cls.get_fields_with_base_type(ObjectId, include_aliases=True)
        if (len(id_fields) > 0):
            for record in records:
                for field in id_fields:
                    if field in record:
                        input = record[field]
                        if (input is not None):
                            record[field] = ObjectId(input)

        return records

    @classmethod
    def write_bulk_unordered(cls, operations: list, chunk_size: int = 10):
        if not operations:
            return

        collection = cls.get_init_collection()
        
        for i in range(0, len(operations), chunk_size):
            chunk = operations[i:i + chunk_size]
            try:
                collection.bulk_write(chunk, ordered=False)
            except BulkWriteError as bwe:
                # If there are errors other than duplicate key (11000), re-raise the original exception.
                # This preserves the full error context for the caller to handle.
                if any(error['code'] != 11000 for error in bwe.details['writeErrors']):
                    raise

    @classmethod
    async def write_bulk_unordered_async(cls, operations: list, chunk_size: int = 10):
        if not operations:
            return

        collection = await cls.get_init_collection_async()
        
        for i in range(0, len(operations), chunk_size):
            chunk = operations[i:i + chunk_size]
            try:
                await collection.bulk_write(chunk, ordered=False)
            except BulkWriteError as bwe:
                # If there are errors other than duplicate key (11000), re-raise the original exception.
                # This preserves the full error context for the caller to handle.
                if any(error['code'] != 11000 for error in bwe.details['writeErrors']):
                    raise

    def handle_duplicate_key_error(self, dke: DuplicateKeyError):
        """
        Handles a duplicate key error.

        Args:
            dke (DuplicateKeyError): The duplicate key error to handle.
        """
        raise dke

    # --- Persistence Methods ---
    def create_persist_operation(self) -> PersistOperation:
        filter = self.self_filter()
        update_instr = self.get_update_instruction()

        if len(filter) > 0:
            return UpdateOneOperation(filter=filter, update=update_instr, upsert=True)
        
        if "$set" in update_instr:
            insert_doc = update_instr["$set"]
            if "$setOnInsert" in update_instr:
                insert_doc.update(update_instr["$setOnInsert"])
            if "$inc" in update_instr:
                for key, value in update_instr["$inc"].items():
                    insert_doc[key] = insert_doc.get(key, 0) + value 
                
            return InsertOneOperation(document=insert_doc)

        raise ValueError("Invalid update instruction", update_instr)
    
    def execute(self, operation: PersistOperation):
        collection = self.get_init_collection()
        
        if isinstance(operation, UpdateOneOperation):
            result = collection.find_one_and_update(
                operation.filter,
                operation.update,
                upsert=operation.upsert,
                return_document=ReturnDocument.AFTER,
            )

            if result is None:
                result = {}
                update_instruction = self.get_update_instruction()
                if update_instruction:
                    # Update the current object with the values from the update instruction
                    if "$set" in update_instruction:
                        result.update(update_instruction["$set"])
                    if "$setOnInsert" in update_instruction:
                        result.update(update_instruction["$setOnInsert"])
            return result

        elif isinstance(operation, InsertOneOperation):
            insert_result = collection.insert_one(operation.document)
            operation.document['_id'] = insert_result.inserted_id
            return operation.document

        raise ValueError("Invalid operation", operation)

    async def execute_async(self, operation: PersistOperation):
        collection = await self.get_init_collection_async()
        
        if isinstance(operation, UpdateOneOperation):
            result = await collection.find_one_and_update(
                operation.filter,
                operation.update,
                upsert=True,
                return_document=ReturnDocument.AFTER,
            )

            if result is None:
                result = {}
                update_instruction = self.get_update_instruction()
                if update_instruction:
                    # Update the current object with the values from the update instruction
                    if "$set" in update_instruction:
                        result.update(update_instruction["$set"])
                    if "$setOnInsert" in update_instruction:
                        result.update(update_instruction["$setOnInsert"])
            return result

        elif isinstance(operation, InsertOneOperation):
            insert_result = await collection.insert_one(operation.document)
            operation.document['_id'] = insert_result.inserted_id
            return operation.document

        raise ValueError("Invalid operation", operation)
        
    def persist(self, lazy: bool = False) -> bool:
        """
        Saves the model to the database.

        If the model does not have an `_id`, a new document will be created.
        Otherwise, the existing document will be updated.
        """

        if lazy and not self.should_persist:
            return False

        persist_operation = self.create_persist_operation()

        try:
            result = self.execute(persist_operation)
            self.on_after_persist(result)
        except DuplicateKeyError as dke:
            self.handle_duplicate_key_error(dke)
            return False
        
        return True

    async def persist_async(self, lazy: bool = False) -> bool:
        if lazy and not self.should_persist:
            return False

        persist_operation = self.create_persist_operation()
        try:
            result = await self.execute_async(persist_operation)
            self.on_after_persist(result)
        except DuplicateKeyError as dke:
            self.handle_duplicate_key_error(dke)
            return False
        
        return True

    @classmethod
    def insert_dataframe(
        cls, dataframe: pd.DataFrame | pl.DataFrame | pyarrow.Table, chunk_size: int = 1000
    ):
        """
        Inserts a DataFrame into the database.

        Note: to have RefreshOnDataframeInsert fields refresh, the columns must exist in the DataFrame.  
        Default columns to None if to have field trigger content.

        Args:
            dataframe (pd.DataFrame): The DataFrame to insert.
        """

        collection = cls.get_init_collection()

        records = cls.convert_dataframe_to_records(dataframe)

        if records is None or len(records) == 0:
            return

        for i in range(0, len(records), chunk_size):
            chunk = records[i:i + chunk_size]
            try:
                collection.insert_many(chunk, ordered=False)  # ordered false so that a duplicate key error won't stop the insert of many
            except BulkWriteError as bwe:
                # If there are errors other than duplicate key (11000), re-raise the original exception.
                # This preserves the full error context for the caller to handle.
                if any(error['code'] != 11000 for error in bwe.details['writeErrors']):
                    raise
                
        return

    @classmethod
    async def insert_dataframe_async(
        cls, dataframe: pd.DataFrame | pl.DataFrame | pyarrow.Table, chunk_size: int = 1000
    ):
        """
        Inserts a pandas DataFrame into the database.

        Note: to have RefreshOnDataframeInsert fields refresh, the columns must exist in the DataFrame.  
        Default columns to None if to have field trigger content.

        Args:
            dataframe (pd.DataFrame): The DataFrame to insert.
        """

        collection = await cls.get_init_collection_async()

        records = cls.convert_dataframe_to_records(dataframe)

        if records is None or len(records) == 0:
            return

        for i in range(0, len(records), chunk_size):
            chunk = records[i:i + chunk_size]
            try:
                await collection.insert_many(chunk, ordered=False)  # ordered false so that a duplicate key error won't stop the insert of many
            except BulkWriteError as bwe:
                # If there are errors other than duplicate key (11000), re-raise the original exception.
                # This preserves the full error context for the caller to handle.
                if any(error['code'] != 11000 for error in bwe.details['writeErrors']):
                    raise
                
        return

    @classmethod
    def update_dataframe(
        cls, dataframe: pd.DataFrame | pl.DataFrame | pyarrow.Table,
        on: list[str] = [],
        upsert: bool = False,
    ):
        """
        Upserts a DataFrame into the database.

        Note: to have RefreshOnDataframeInsert fields refresh, the columns must exist in the DataFrame.  
        Default columns to None if to have field trigger content.

        Args:
            dataframe (pd.DataFrame): The DataFrame to update or insert.
            on (list[str]): The fields to use for upserting.
        """

        collection = cls.get_init_collection()

        records = cls.convert_dataframe_to_records(dataframe)

        if records is None or len(records) == 0:
            return

        operations: list = []

        for record in records:
            filter = {key: record[key] for key in on}
            update = {"$set": record}
            update_op = UpdateMany(
                filter,
                update,
                upsert=upsert,
            )
            operations.append(update_op)
        
        cls.write_bulk_unordered(operations)


    @classmethod
    async def update_dataframe_async(
        cls, dataframe: pd.DataFrame | pl.DataFrame | pyarrow.Table,
        on: list[str] = [],
        upsert: bool = False,
    ):
        """
        Upserts a DataFrame into the database.

        Note: to have RefreshOnDataframeInsert fields refresh, the columns must exist in the DataFrame.  
        Default columns to None if to have field trigger content.

        Args:
            dataframe (pd.DataFrame): The DataFrame to update or insert.
            on (list[str]): The fields to use for upserting.
        """

        records = cls.convert_dataframe_to_records(dataframe)

        if records is None or len(records) == 0:
            return

        operations: list = []

        for record in records:
            filter = {key: record[key] for key in on}
            update = {"$set": record}
            update_op = UpdateMany(
                filter,
                update,
                upsert=upsert,
            )
            operations.append(update_op)
        
        await cls.write_bulk_unordered_async(operations)

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

        filter = QueryableField("id") == id
        return cls.filter(filter).load_one()

    @classmethod
    async def from_id_async(cls, id: ObjectId | str):
        if isinstance(id, str):
            if not ObjectId.is_valid(id):
                return None
            id = ObjectId(id)

        filter = QueryableField("id") == id
        return await cls.filter(filter).load_one_async()
    
    @classmethod
    def get_mql_driver(cls) -> ExpressionDriver:
        return ExpressionDriver(cls.model_fields)


    # --- Database and Collection Configuration ---

    @classmethod
    def filter(cls: Type[Self], filter: QueryPredicates = QueryPredicates()) -> LoadDirective[Self]:
        return LoadDirective(cls).filter(filter)

    @classmethod
    def agg(cls: Type[Self], aggregation: AggregationStages = AggregationStages()) -> LoadDirective[Self]:
        return LoadDirective(cls).agg(aggregation)

class Persistable(PersistableBase):
    updated_time: TimeUpdated = Field(
        description="Timestamp of the last update.", default=None
    )
    
    created_at: TimeInserted = Field(
        description="Entity Created Time (does not exist if entity has not been persisted)",
        default=None,
    )

def declare_persist_db(
    collection_name: str,
    db_name: str,
    client_factory: DbClientFactory = LocalClientFactory(),
    version: Optional[int] = None,
    index: Optional[List[Index]] = None,
    test: bool = False,
):
    """
    A class decorator that declares how a model should be persisted to
    MongoDB.

    Args:
        collection_name (str): The name of the MongoDB collection.
        db_name (str): The name of the MongoDB database.
        client_factory (DbClientFactory, optional): The client factory to use for the database. Defaults to `LocalDbClientFactory()`.
        test (bool, optional): Whether the model is a test model. Defaults
            to `False`.
    """

    def decorator(cls):
        cls._db_model_driver = MongoDbModelDriver(
            collection_name=collection_name,
            db_name=db_name,
            client_factory=client_factory,
            version=version,
            index=index,
            test=test,
        )
        return cls

    return decorator
