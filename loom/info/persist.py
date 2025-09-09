from typing import Any, Optional
from bson import ObjectId
import pandas as pd
from pydantic import Field, PrivateAttr
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database
from loom.info.aggregation import Aggregation
from loom.info.universal import get_remote_db_client, get_local_db_client
from loom.info.model import CoalesceOnInsert, Model, QueryableTransformer, TimeUpdated, TimeInserted, coalesce
from loom.info.filter import Filter, Size, SortDesc, SortOp


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
            when the model is modified.
        created_at (TimeInserted): A timestamp that is set once when the model
            is first inserted into the database.
        _has_update (bool): A private flag indicating if the model has pending
            changes to be persisted. Defaults to `True` on creation.
    """

    updated_time: TimeUpdated = Field(description="Entity Time", default=None)
    created_at: TimeInserted = Field(description="Entity Created Time (does not exist if entity has not been persisted)", default=None)

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

    def self_filter(self) -> Filter:
        """
        Returns a filter to find the current document in the database by its ID.

        Returns:
            Filter: A filter expression for finding the document by its `_id`.
        """
        return Filter({"_id": self.collapse_id()})
    
    def get_set_on_insert_instruction(self) -> dict:
        """
        Constructs the `$setOnInsert` part of a MongoDB update operation.

        This includes the model version and any fields marked with
        `CoalesceOnInsert` that should only be set when a document is first
        created.

        Returns:
            dict: The `$setOnInsert` dictionary for an update operation.
        """
        model_version = self.get_model_code_version()
        set_on_insert_op : dict = {}
        if model_version is not None:
            set_on_insert_op["version"] = model_version

        for field, transformers in self.get_field_hints(CoalesceOnInsert).items():
            set_on_insert_op[field] = coalesce(getattr(self, field), transformers)

        return set_on_insert_op

    def get_update_instruction(self) -> dict:
        """
        Constructs a complete MongoDB update instruction for an upsert operation.

        This method separates fields into a `$set` operation for updates and a
        `$setOnInsert` operation for fields that should only be written on
        document creation.

        Returns:
            dict: A dictionary representing the full update instruction.
        """
        doc = self.dump_doc()
        update_instr: dict[str, Any] = {}
        set_on_insert_op: dict[str, Any] = self.get_set_on_insert_instruction()
        delete_field = [field for field in set_on_insert_op.keys()]
        for field in delete_field:
            if field in doc:
                del doc[field]

        if len(doc) > 0:
            update_instr["$set"] = doc

        update_instr["$setOnInsert"] = set_on_insert_op
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
            Exception: If `collection_name` is not defined in the metadata.
        """
        db_info = cls.get_db_info()
        name = db_info.get("collection_name")
        if not name:
            raise Exception(
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
            Exception: If `db_name` is not defined in the metadata.
        """
        db_info = cls.get_db_info()
        name = db_info.get("db_name")
        if not name:
            raise Exception(
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
    def get_db_collection(
        cls
    ) -> Collection:
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

    # --- Querying from persistence ---
    @classmethod
    def parse_filter(cls, filter: Filter | dict) -> dict:
        """
        Parses a `Filter` object or dict into a MongoDB query dictionary.

        This method also applies any `QueryableTransformer` annotations on the
        model's fields to transform corresponding values in the filter.

        Args:
            filter (Filter | dict): The filter expression to parse.

        Returns:
            dict: A MongoDB-compatible query dictionary.
        """
        meta_map = {
            key: [
                item
                for item in value.metadata
                if isinstance(item, QueryableTransformer)
            ]
            for key, value in cls.model_fields.items()
        }
        before_query_map = {
            key: value for key, value in meta_map.items() if len(value) > 0
        }

        retval: dict = filter.get_exp() if isinstance(filter, Filter) else filter
        if len(before_query_map) == 0:
            return retval

        for key, before_query in before_query_map.items():
            if key in retval:
                for transformer in before_query:
                    retval[key] = transformer(retval[key])

        return retval

    @classmethod
    def parse_agg_stage(cls, stage: str, expr) -> dict:
        """
        Parses a single stage of an aggregation pipeline.

        If the stage is `$match`, it uses `parse_filter` to process the expression.

        Args:
            stage (str): The aggregation stage (e.g., `'$match'`).
            expr: The expression for the stage.

        Returns:
            dict: A dictionary representing the aggregation stage.
        """
        if stage == "$match":
            return {"$match": cls.parse_filter(expr)}

        return {stage: expr}

    @classmethod
    def parse_agg_pipe(cls, aggregation: Aggregation) -> list[dict]:
        """
        Parses an `Aggregation` object into a MongoDB aggregation pipeline.

        Args:
            aggregation (Aggregation): The `Aggregation` object to parse.

        Returns:
            list[dict]: A list of dictionaries representing the pipeline.
        """
        return [cls.parse_agg_stage(stage, expr) for stage, expr in aggregation]

    @classmethod
    def load_one(
        cls,
        filter: Filter = Filter(),
        sort: SortOp = SortOp(),
        **kwargs,
    ):
        """
        Loads a single document from the database that matches the filter.

        Args:
            filter (Filter, optional): A filter to apply to the query.
            sort (SortOp, optional): A sort directive.
            **kwargs: Additional keyword arguments to form a filter.

        Returns:
            Optional[Self]: An instance of the model, or `None` if no document
                is found.
        """
        collection = cls.get_db_collection()

        filter &= Filter(kwargs)

        doc = collection.find_one(cls.parse_filter(filter), sort=sort.get_tuples())
        return cls.from_db_doc(doc) if doc else None

    @classmethod
    def load_latest(
        cls,
        filter: Filter = Filter(),
        sort: SortOp = SortDesc("updated_time"),
        **kwargs,
    ):
        """
        Loads the most recently updated document from the database.

        Args:
            filter (Filter, optional): A filter to apply to the query.
            sort (SortOp, optional): Sort order. Defaults to `updated_time`
                descending.
            **kwargs: Additional keyword arguments to pass to `load_one`.

        Returns:
            Optional[Self]: An instance of the loaded document, or `None` if not
                found.
        """

        return cls.load_one(filter, sort=sort, **kwargs)

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

        return cls.load_one(_id=id)

    @classmethod
    def aggregate(
        cls,
        aggregation: Optional[Aggregation] = None,
        filter: Filter = Filter(),
        sampling: Optional[Size] = None,
        sort: SortOp = SortOp(),
        **kwargs,
    ):
        """
        Performs an aggregation query on the model's collection.

        Args:
            aggregation (Aggregation, optional): The aggregation pipeline to
                execute.
            filter (Filter, optional): A filter to apply before the aggregation.
            sampling (Optional[Size], optional): The number of documents to
                randomly sample.
            sort (SortOp, optional): A sort directive to apply after the
                aggregation.
            **kwargs: Additional keyword arguments to form a filter.

        Returns:
            CommandCursor: A `pymongo` cursor to the results of the aggregation.
        """
        collection = cls.get_db_collection()

        if aggregation is None:
            aggregation = Aggregation()

        filter &= Filter(kwargs)
        if filter.has_filter():
            aggregation = aggregation.Match(filter)

        if sampling:
            aggregation = aggregation.Sample(sampling)

        aggregation = aggregation.Sort(sort)

        return collection.aggregate(cls.parse_agg_pipe(aggregation))

    @classmethod
    def load_aggregate(
        cls,
        aggregation: Optional[Aggregation] = None,
        filter: Filter = Filter(),
        sampling: Optional[Size] = None,
        sort: SortOp = SortOp(),
        **kwargs,
    ):
        """
        Executes an aggregation and returns the results as a list of models.

        Args:
            aggregation (Aggregation, optional): The aggregation pipeline.
            filter (Filter, optional): A filter to apply to the aggregation.
            sampling (Optional[Size], optional): The number of documents to
                sample.
            sort (SortOp, optional): A sort directive for the aggregation.
            **kwargs: Additional keyword arguments to form a filter.

        Returns:
            list[Self]: A list of model instances.
        """
        with cls.aggregate(
            aggregation=aggregation,
            filter=filter,
            sampling=sampling,
            sort=sort,
            **kwargs,
        ) as cursors:
            return [cls.from_db_doc(doc) for doc in cursors]

    @classmethod
    def load_many(
        cls,
        filter: Filter = Filter(),
        limit: Size = Size(0),
        sort: SortOp = SortOp(),
        **kwargs,
    ):
        """
        Loads multiple documents from the database.

        Args:
            filter (Filter, optional): A filter to apply to the query.
            limit (Size, optional): The maximum number of documents to load.
                Defaults to `0` (no limit).
            sort (SortOp, optional): A sort directive.
            **kwargs: Additional keyword arguments to form a filter.

        Returns:
            list[Self]: A list of loaded model instances.
        """
        collection = cls.get_db_collection()

        filter &= Filter(kwargs)

        with collection.find(
            cls.parse_filter(filter), limit=limit.get_exp(), sort=sort.get_tuples()
        ) as cursor:
            return [cls.from_db_doc(doc) for doc in cursor]

    @classmethod
    def load_dataframe(
        cls,
        aggregation: Optional[Aggregation] = None,
        filter: Filter = Filter(),
        sampling: Optional[Size] = None,
        sort: SortOp = SortOp(),
        **kwargs,
    ) -> pd.DataFrame:
        """
        Loads data from an aggregation query into a pandas DataFrame.

        Args:
            aggregation (Aggregation, optional): The aggregation pipeline.
            filter (Filter, optional): A filter to apply to the aggregation.
            sampling (Optional[Size], optional): The number of documents to
                sample.
            sort (SortOp, optional): A sort directive for the aggregation.
            **kwargs: Additional keyword arguments to form a filter.

        Returns:
            pd.DataFrame: A pandas DataFrame containing the loaded data.
        """
        with cls.aggregate(
            aggregation=aggregation,
            filter=filter,
            sampling=sampling,
            sort=sort,
            **kwargs,
        ) as cursor:
            return pd.DataFrame(cursor)

    @classmethod
    def exists(cls, **kwargs) -> bool:
        """
        Checks if at least one document matching the filter exists.

        Args:
            **kwargs: Keyword arguments to use as a filter.

        Returns:
            bool: `True` if a matching document exists, `False` otherwise.
        """
        filter = Filter(kwargs)
        exists = cls.get_db_collection().find_one(cls.parse_filter(filter))
        if exists is None:
            return False
        return True

    # ---END: Querying from persistence ---
    
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