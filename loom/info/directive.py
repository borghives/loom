from typing import Generic, Optional, Type
from pymongoarrow.api import ( #type: ignore
    Schema,  Table,
    aggregate_arrow_all,
    aggregate_pandas_all,
    aggregate_polars_all
) 

import pandas as pd
import polars as pl

from loom.info.aggregation import Aggregation
from loom.info.filter import Filter
from loom.info.sort_op import SortDesc, SortOp, SortAsc
from loom.info.persistable import PersistableType

class LoadDirective(Generic[PersistableType]):
    """
    A directive for loading data from a `Persistable` model.

    This class provides a fluent API for building queries to load data from a MongoDB collection.
    It supports filtering, sorting, limiting, and aggregation.
    """
    def __init__(self, persistable: Type[PersistableType]) -> None:
        self._aggregation_expr: Aggregation = Aggregation()
        self._persist_cls: Type[PersistableType] = persistable

    def filter(self, filter: Filter) -> "LoadDirective[PersistableType]":
        """
        Adds a filter to the query.

        Args:
            filter (Filter): The filter to add.

        Returns:
            LoadDirective: The `LoadDirective` object for chaining.
        """
        self._aggregation_expr = self._aggregation_expr.match(filter)
        return self

    def sort(self, sort: SortOp | str, descending: bool = False) -> "LoadDirective[PersistableType]":
        """
        Adds a sort to the query.

        Args:
            sort (SortOp | str): The sort to add.
            descending (bool): Whether to sort in descending order.  Only significant is sort is a string. Defaults to `False`.

        Returns:
            LoadDirective: The `LoadDirective` object for chaining.
        """
        if isinstance(sort, SortOp):
            sort_op = sort
        else:
            sort_op = SortDesc(sort) if descending else SortAsc(sort)

        self._aggregation_expr = self._aggregation_expr.sort(sort_op)
        return self
    
    def skip(self, skip: int) -> "LoadDirective[PersistableType]":
        """
        Adds a skip to the query.

        Args:
            skip (int): The number of documents to skip.

        Returns:
            LoadDirective: The `LoadDirective` object for chaining.
        """
        self._aggregation_expr = self._aggregation_expr.skip(skip)
        return self
    
    def limit(self, limit: int) -> "LoadDirective[PersistableType]":
        """
        Adds a limit to the query.

        Args:
            limit (int): The limit to add.

        Returns:
            LoadDirective: The `LoadDirective` object for chaining.
        """
        self._aggregation_expr = self._aggregation_expr.limit(limit)
        return self
    
    def sample(self, sample: int) -> "LoadDirective[PersistableType]":
        """
        Adds a sample stage to the aggregation pipeline.

        Args:
            sample (int): The number of documents to sample.

        Returns:
            LoadDirective: The `LoadDirective` object for chaining.
        """
        self._aggregation_expr = self._aggregation_expr.sample(sample)
        return self
    
    def count(self) -> int:
        """
        Executes a count query on the model's collection.

        Returns:
            int: The number of documents matching the filter.
        """
        self._aggregation_expr = self._aggregation_expr.count("count")
        with self.exec_aggregate() as cursors:
            for result in cursors:
                return result.get("count", 0)
        return 0

    
    def aggregation(self, aggregation: Aggregation) -> "LoadDirective[PersistableType]":
        """
        Adds an aggregation pipeline to the query.

        Args:
            aggregation (Aggregation): The aggregation pipeline to add.

        Returns:
            LoadDirective: The `LoadDirective` object for chaining.
        """
        self._aggregation_expr |= aggregation
        return self
    
    def exec_aggregate(self : "LoadDirective[PersistableType]", post_agg: Optional[Aggregation] = None):
        """
        Performs an aggregation query on the model's collection.

        Args:
            post_agg (Aggregation, optional): The aggregation pipeline to
                append to the end just for this execution

        Returns:
            CommandCursor: A `pymongo` cursor to the results of the aggregation.
        """
        p_cls = self._persist_cls
        collection = p_cls.get_init_collection()
        return collection.aggregate(self.get_pipeline_expr(post_agg))

    def load_aggregate(self, post_agg: Optional[Aggregation] = None):
        """
        Executes an aggregation and returns the results as a list of models.

        Args:
            post_agg (Aggregation, optional): The aggregation pipeline to
                append to the end just for this execution

        Returns:
            list[Self]: A list of model instances.
        """
        p_cls = self._persist_cls
        with self.exec_aggregate(post_agg) as cursors:
            return [p_cls.from_doc(doc) for doc in cursors]
    
    def load_one(self):
        """
        Loads a single document from the database.

        Returns:
            Optional[Persistable]: An instance of the model, or `None` if no document is found.
        """
        docs = self.load_aggregate(Aggregation().limit(1))
        return docs[0] if len(docs) > 0 else None
    
    def load_many(self):
        """
        Loads multiple documents from the database.

        Returns:
            list[Persistable]: A list of loaded model instances.
        """
        return self.load_aggregate()
    
    def load_latest(self, sort: SortOp = SortDesc("updated_time")):
        """
        Loads the most recently updated document from the database.

        Args:
            sort (SortOp, optional): Sort order. Defaults to `updated_time` descending.

        Returns:
            Optional[Persistable]: An instance of the loaded document, or `None` if not found.
        """
        docs = self.load_aggregate(Aggregation().sort(sort).limit(1))
        return docs[0] if len(docs) > 0 else None
    
    async def exec_aggregate_async(self, post_agg: Optional[Aggregation] = None):
        p_cls = self._persist_cls
        collection = await p_cls.get_init_collection_async()
        return await collection.aggregate(self.get_pipeline_expr(post_agg))

    async def load_aggregate_async(self : "LoadDirective[PersistableType]", post_agg: Optional[Aggregation] = None):
        p_cls = self._persist_cls
        cursor = await self.exec_aggregate_async(post_agg)
        async with cursor:
            return [p_cls.from_doc(doc) async for doc in cursor]

    async def load_one_async(self):
        docs = await self.load_aggregate_async(Aggregation().limit(1))
        return docs[0] if len(docs) > 0 else None
    
    async def load_many_async(self):
        return await self.load_aggregate_async()

    async def load_latest_async(self, sort: SortOp = SortDesc("updated_time")):
        docs = await self.load_aggregate_async(Aggregation().sort(sort).limit(1))
        return docs[0] if len(docs) > 0 else None
    
    def exists(self) -> bool:
        """
        Checks if at least one document matching the filter exists.

        Returns:
            bool: `True` if a matching document exists, `False` otherwise.
        """
        docs = self.load_aggregate(Aggregation().limit(1))
        return len(docs) > 0

    async def exists_async(self) -> bool:
        docs = await self.load_aggregate_async(Aggregation().limit(1))
        return len(docs) > 0
    
    def load_table(self, schema: Optional[Schema] = None) -> Table:
        """
        Loads data from a query into a PyArrow Table.

        Args:
            schema (Schema, optional): The PyArrow schema to use.

        Returns:
            Table: A PyArrow Table containing the loaded data.
        """
        p_cls = self._persist_cls
        collection = p_cls.get_db_collection()
        return aggregate_arrow_all(collection, pipeline=self.get_pipeline_expr(), schema=schema)
    
    def load_dataframe(self, schema: Optional[Schema] = None) -> pd.DataFrame:
        """
        Loads data from a query into a pandas DataFrame.

        Args:
            schema (Schema, optional): The PyArrow schema to use.

        Returns:
            pd.DataFrame: A pandas DataFrame containing the loaded data.
        """
        p_cls = self._persist_cls
        collection = p_cls.get_db_collection()
        return aggregate_pandas_all(collection, pipeline=self.get_pipeline_expr(), schema=schema)

    def load_polars(self, schema: Optional[Schema] = None) -> pl.DataFrame | pl.Series:
        """
        Loads data from a query into a polars DataFrame.

        Args:
            schema (Schema, optional): The PyArrow schema to use.

        Returns:
            pl.DataFrame | pl.Series: A polars DataFrame or Series containing the loaded data.
        """
        p_cls = self._persist_cls
        collection = p_cls.get_db_collection()
        return aggregate_polars_all(collection, pipeline=self.get_pipeline_expr(), schema=schema)

    def _load_dataframe_legacy(
        self,
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
        with self.exec_aggregate() as cursors:
            df = pd.DataFrame(cursors)
            return df
        
    def get_pipeline_expr(self, post_agg: Optional[Aggregation] = None) -> list[dict]:
        return (self._aggregation_expr | post_agg).pipeline()
