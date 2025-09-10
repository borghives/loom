from typing import Optional

from pymongo import InsertOne

from loom.info.model import CoalesceOnInsert, RefreshOnSet
from loom.info.persist import Persistable

TIMESERIES_META_NAME = "_db_series_metadata"

class LedgerModel(Persistable):
    """
    A non-destructive persistence model that creates a new document for each
    `persist` call.

    This model is useful for creating immutable records, where each save
    operation results in a new entry in the database.
    """

    def persist(self, lazy: bool = False) -> bool:
        """
        Saves the model to the database as a new document.

        Args:
            override_client (Optional[MongoClient], optional): A MongoDB client
                to use instead of the default client. Defaults to `None`.
        """

        if lazy and not self.should_persist:
            return False

        collection = self.get_db_collection()

        self.coalesce_fields_for(CoalesceOnInsert)
        self.coalesce_fields_for(RefreshOnSet)        
        result = collection.insert_one(self.dump_doc())
        if result:
            self._id = result.inserted_id

        self.has_update = False
        return True
    
    @classmethod
    def persist_many(cls, items: list, lazy: bool = False) -> None:
        """
        Saves multiple models to the database as new documents.

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
            item.coalesce_fields_for(CoalesceOnInsert)
            item.coalesce_fields_for(RefreshOnSet)  
            insert_op = InsertOne(item.dump_doc())
            operations.append(insert_op)

        collection = cls.get_db_collection()
        collection.bulk_write(operations)

class TimeSeriesLedgerModel(LedgerModel):
    """
    A ledger model that is specifically designed for time-series data.

    This model extends `LedgerModel` and provides additional features for
    creating and managing time-series collections in MongoDB.
    """

    # --- Information on the timeseries the model ---
    @classmethod
    def get_timeseries_info(cls) -> dict:
        """
        Gets the time-series information for the model.

        This information is used to create the time-series collection in
        MongoDB.

        Returns:
            dict: The time-series information for the model.
        """

        if not hasattr(cls, TIMESERIES_META_NAME):
            raise Exception(
                f"Class {cls.__name__} is not decorated with @declare_timeseries"
            )
        
        return getattr(cls, TIMESERIES_META_NAME)

    @classmethod
    def create_collection(cls) -> None:
        """
        Creates the time-series collection in MongoDB.
        """
        db = cls.get_db()
        collection_names = db.list_collection_names()
        name = cls.get_db_collection_name()

        timeseries = {
            "timeField": "updated_time",
        }

        series_info = cls.get_timeseries_info()
        granularity = series_info.get("granularity")
        if granularity:
            timeseries["granularity"] = granularity

        metafield = series_info.get("metakey")
        if metafield:
            timeseries["metaField"] = metafield

        ttl = series_info.get("ttl")
        if name not in collection_names:
            db.create_collection(
                name, timeseries=timeseries, expireAfterSeconds=ttl
            )
            
def declare_timeseries(
    metakey: Optional[str] = None,
    granularity: Optional[str] = None,
    ttl: Optional[int] = None,
):
    """
    A class decorator that declares how a model should be persisted to
    MongoDB.

    Args:
        metakey (Optional[str], optional): The name of the meta key for
            time-series collections. Defaults to `None`.
        granularity (Optional[str], optional): The granularity of
            time-series collections. Can be `"seconds"`, `"minutes"`, or
            `"hours"`. Defaults to `None`.
        ttl (Optional[int], optional): The TTL of time-series
            collections in seconds. Defaults to `None`.
    """

    def decorator(cls):
        if granularity:
            if granularity not in ["seconds", "minutes", "hours"]:
                raise Exception(
                    f"Invalid timeseries granularity {granularity}.  Must be one of seconds, minutes, or hours"
                )

        metadata = {}
        metadata["metakey"] = metakey
        metadata["granularity"] = granularity
        metadata["ttl"] = ttl
        setattr(cls, TIMESERIES_META_NAME, metadata)
        return cls

    return decorator