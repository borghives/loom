from typing import Optional

from pymongo import InsertOne

from loom.info.model import CoalesceOnInsert, RefreshOnSet
from loom.info.persistable import Persistable

TIMESERIES_META_NAME = "_db_series_metadata"

class LedgerModel(Persistable):
    """
    Represents a non-destructive, append-only persistence model.

    Each call to `persist` creates a new document in the database, making it
    ideal for creating immutable records or logs. This behavior is analogous
    to a ledger where new entries are always added without modifying existing
    ones.
    """

    def persist(self, lazy: bool = False) -> bool:
        """
        Saves the current state of the model as a new document in the database.

        If `lazy` is True, the operation is skipped if the model has no pending
        updates (`should_persist` is False).

        Args:
            lazy (bool): If True, persist only if there are pending changes.
                         Defaults to False.

        Returns:
            bool: True if the document was saved, False otherwise.
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
        Saves a list of model instances as new documents in a single bulk operation.

        This method is more efficient than calling `persist` on each item
        individually.

        Args:
            items (list): A list of model instances to save.
            lazy (bool): If True, only persist items that have pending changes.
                         Defaults to False.
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
    A ledger model specifically designed for MongoDB time-series collections.

    This model extends `LedgerModel` and works in conjunction with the
    `@declare_timeseries` decorator to configure and create time-series
    collections automatically. It assumes that documents will be stored in a
    collection optimized for data points recorded over time.
    """

    # --- Information on the timeseries the model ---
    @classmethod
    def get_timeseries_info(cls) -> dict:
        """
        Retrieves the time-series configuration for the model class.

        This configuration is defined by the `@declare_timeseries` decorator.

        Returns:
            dict: A dictionary containing the time-series configuration,
                  including metakey, granularity, and TTL.

        Raises:
            Exception: If the class is not decorated with `@declare_timeseries`.
        """

        if not hasattr(cls, TIMESERIES_META_NAME):
            raise Exception(
                f"Class {cls.__name__} is not decorated with @declare_timeseries"
            )
        
        return getattr(cls, TIMESERIES_META_NAME)

    @classmethod
    def create_collection(cls) -> None:
        """
        Creates a time-series collection in MongoDB based on the model's
        declaration.

        If the collection does not already exist, it will be created using the
        parameters specified in the `@declare_timeseries` decorator. The time
        field is automatically set to 'updated_time'.
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
    A class decorator to configure a `TimeSeriesLedgerModel` for a MongoDB
    time-series collection.

    This decorator attaches the necessary metadata to the model class, which is
    then used by `TimeSeriesLedgerModel.create_collection` to set up the
    database collection correctly.

    Example:
        @declare_timeseries(metakey='device_id', granularity='minutes', ttl=3600)
        class SensorData(TimeSeriesLedgerModel):
            device_id: str
            temperature: float

    Args:
        metakey (Optional[str], optional): The name of the field that contains
            metadata for the time-series. This is used as the `metaField` in
            MongoDB. Defaults to `None`.
        granularity (Optional[str], optional): The granularity of the
            time-series data. Valid options are `"seconds"`, `"minutes"`, or
            `"hours"`. Defaults to `None`.
        ttl (Optional[int], optional): The time-to-live (TTL) for documents in
            the collection, specified in seconds. Documents will automatically
            be deleted after this duration. Defaults to `None`.
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