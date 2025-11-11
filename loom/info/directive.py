from typing import Generic, List, Optional, Type
from pymongoarrow.api import ( #type: ignore
    Schema,  Table,
    aggregate_arrow_all,
    aggregate_pandas_all,
    aggregate_polars_all
) 

import pandas as pd
import polars as pl

from loom.info.aggregation import AggregationStages
from loom.info.expression import Expression, FieldPath, GroupExpression, FieldSpecification
from loom.info.filter import QueryPredicates
from loom.info.sort_op import SortDesc, SortOp, SortAsc
from loom.info.persistable import PersistableType

class LoadDirective(Generic[PersistableType]):
    """
    A directive for loading data from a `Persistable` model.

    This class provides a fluent API for building queries to load data from a MongoDB collection.
    It supports filtering, sorting, limiting, and aggregation.
    """
    def __init__(self, persistable: Type[PersistableType]) -> None:
        self._aggregation_expr: AggregationStages = AggregationStages()
        self._persist_cls: Type[PersistableType] = persistable

    def filter(self : "LoadDirective[PersistableType]", filter: QueryPredicates) -> "LoadDirective[PersistableType]":
        """
        Adds a filter to the query.

        Args:
            filter (Filter): The filter to add.

        Returns:
            LoadDirective: The `LoadDirective` object for chaining.
        """
        self._aggregation_expr = self._aggregation_expr.match(filter)
        return self

    def sort(self : "LoadDirective[PersistableType]", sort: Optional[SortOp | str], descending: bool = False) -> "LoadDirective[PersistableType]":
        """
        Adds a sort to the query.

        Args:
            sort (SortOp | str): The sort to add.
            descending (bool): Whether to sort in descending order.  Only significant is sort is a string. Defaults to `False`.

        Returns:
            LoadDirective: The `LoadDirective` object for chaining.
        """

        if sort is None:
            return self

        if isinstance(sort, SortOp):
            sort_op = sort
        else:
            sort_op = SortDesc(sort) if descending else SortAsc(sort)

        self._aggregation_expr = self._aggregation_expr.sort(sort_op)
        return self
    
    def skip(self : "LoadDirective[PersistableType]", skip: int) -> "LoadDirective[PersistableType]":
        """
        Adds a skip to the query.

        Args:
            skip (int): The number of documents to skip.

        Returns:
            LoadDirective: The `LoadDirective` object for chaining.
        """
        self._aggregation_expr = self._aggregation_expr.skip(skip)
        return self
    
    def add_fields(self : "LoadDirective[PersistableType]", add_fields: dict) -> "LoadDirective[PersistableType]":
        """
        Adds an addFields stage to the aggregation pipeline.

        Args:
            add_fields (dict): The fields to add.

        Returns:
            LoadDirective: The `LoadDirective` object for chaining.
        """
        self._aggregation_expr = self._aggregation_expr.add_fields(add_fields)
        return self

    def limit(self : "LoadDirective[PersistableType]", limit: int) -> "LoadDirective[PersistableType]":
        """
        Adds a limit to the query.

        Args:
            limit (int): The limit to add.

        Returns:
            LoadDirective: The `LoadDirective` object for chaining.
        """
        self._aggregation_expr = self._aggregation_expr.limit(limit)
        return self
    
    def sample(self : "LoadDirective[PersistableType]", sample: int) -> "LoadDirective[PersistableType]":
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
        with self.exec_agg() as cursors:
            for result in cursors:
                return result.get("count", 0)
        return 0

    def project(self : "LoadDirective[PersistableType]", *specifications: FieldSpecification | dict) -> "LoadDirective[PersistableType]":
        combined = None
        for specification in specifications:
            if combined is None:
                combined = specification
            elif isinstance(combined, FieldSpecification):
                assert isinstance(specification, FieldSpecification) 
                combined |= specification
            elif isinstance(combined, dict):
                assert isinstance(specification, dict)
                combined |= specification
        
        if combined is not None:
            self._aggregation_expr = self._aggregation_expr.project(combined)

        return self

    def group_by(self : "LoadDirective[PersistableType]", key: str | Expression) -> "GroupDirective[PersistableType]":
        if isinstance(key, str):
            key = FieldPath(key)
        assert isinstance(key, Expression)
        return GroupDirective[PersistableType](self, key)
    
    def agg(self : "LoadDirective[PersistableType]", aggregation: AggregationStages) -> "LoadDirective[PersistableType]":
        """
        Adds an aggregation pipeline to the query.

        Args:
            aggregation (Aggregation): The aggregation pipeline to add.

        Returns:
            LoadDirective: The `LoadDirective` object for chaining.
        """
        self._aggregation_expr |= aggregation
        return self
    
    def exec_agg(self : "LoadDirective[PersistableType]", post_agg: Optional[AggregationStages] = None):
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

    def load_agg(self : "LoadDirective[PersistableType]", post_agg: Optional[AggregationStages] = None):
        """
        Executes an aggregation and returns the results as a list of models.

        Args:
            post_agg (Aggregation, optional): The aggregation pipeline to
                append to the end just for this execution

        Returns:
            list[Self]: A list of model instances.
        """
        p_cls = self._persist_cls
        with self.exec_agg(post_agg) as cursors:
            return [p_cls.from_doc(doc) for doc in cursors]
    
    def load_one(self: "LoadDirective[PersistableType]"):
        """
        Loads a single document from the database.

        Returns:
            Optional[Persistable]: An instance of the model, or `None` if no document is found.
        """
        docs = self.load_agg(AggregationStages().limit(1))
        return docs[0] if len(docs) > 0 else None
    
    def load_many(self: "LoadDirective[PersistableType]"):
        """
        Loads multiple documents from the database.

        Returns:
            list[Persistable]: A list of loaded model instances.
        """
        return self.load_agg()
    
    def load_latest(self: "LoadDirective[PersistableType]", sort: SortOp = SortDesc("updated_time")):
        """
        Loads the most recently updated document from the database.

        Args:
            sort (SortOp, optional): Sort order. Defaults to `updated_time` descending.

        Returns:
            Optional[Persistable]: An instance of the loaded document, or `None` if not found.
        """
        docs = self.load_agg(AggregationStages().sort(sort).limit(1))
        return docs[0] if len(docs) > 0 else None
    
    def merge_into(self: "LoadDirective[PersistableType]", collection_name: str, on: List[str]):
        self._aggregation_expr = self._aggregation_expr.merge({
            "into": collection_name,
            "on": on,
            "whenMatched": "replace", #<replace|keepExisting|merge|fail|pipeline>, 
            "whenNotMatched": "insert" #<insert|discard|fail> 
        })
        self.exec_agg()
    

    async def exec_agg_async(self: "LoadDirective[PersistableType]", post_agg: Optional[AggregationStages] = None):
        p_cls = self._persist_cls
        collection = await p_cls.get_init_collection_async()
        return await collection.aggregate(self.get_pipeline_expr(post_agg))

    async def load_agg_async(self : "LoadDirective[PersistableType]", post_agg: Optional[AggregationStages] = None):
        p_cls = self._persist_cls
        cursor = await self.exec_agg_async(post_agg)
        async with cursor:
            return [p_cls.from_doc(doc) async for doc in cursor]

    async def load_one_async(self: "LoadDirective[PersistableType]"):
        docs = await self.load_agg_async(AggregationStages().limit(1))
        return docs[0] if len(docs) > 0 else None
    
    async def load_many_async(self: "LoadDirective[PersistableType]"):
        return await self.load_agg_async()

    async def load_latest_async(self: "LoadDirective[PersistableType]", sort: SortOp = SortDesc("updated_time")):
        docs = await self.load_agg_async(AggregationStages().sort(sort).limit(1))
        return docs[0] if len(docs) > 0 else None
    
    async def merge_into_async(self: "LoadDirective[PersistableType]", collection_name: str, on: List[str]):
        self._aggregation_expr = self._aggregation_expr.merge({
            "into": collection_name,
            "on": on,
            "whenMatched": "replace", #<replace|keepExisting|merge|fail|pipeline>, 
            "whenNotMatched": "insert" #<insert|discard|fail> 
        })
        await self.exec_agg_async()
    
    def exists(self) -> bool:
        """
        Checks if at least one document matching the filter exists.

        Returns:
            bool: `True` if a matching document exists, `False` otherwise.
        """
        docs = self.load_agg(AggregationStages().limit(1))
        return len(docs) > 0

    async def exists_async(self) -> bool:
        docs = await self.load_agg_async(AggregationStages().limit(1))
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
        collection = p_cls.get_init_collection()
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
        collection = p_cls.get_init_collection()
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
        collection = p_cls.get_init_collection()
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
        with self.exec_agg() as cursors:
            df = pd.DataFrame(cursors)
            return df
        
    def get_pipeline_expr(self, post_agg: Optional[AggregationStages] = None) -> list[dict]:
        pipelines = (self._aggregation_expr | post_agg)
        flattened_pipelines =pipelines.express(self._persist_cls.get_mql_driver())
        assert isinstance(flattened_pipelines, list)
        return flattened_pipelines


class GroupDirective(Generic[PersistableType]):
    def __init__(self, base_directive: LoadDirective[PersistableType], key: Expression):
        self._base_directive = base_directive
        self.group_expression = GroupExpression(key)
        
    def acc(self : "GroupDirective[PersistableType]", *accumulators: FieldSpecification) -> LoadDirective[PersistableType]:
        combined = None
        for accumulator in accumulators:
            if combined is None:
                combined = accumulator
            else:
                combined |= accumulator
        
        if combined is not None:
            group_agg = AggregationStages().group(self.group_expression.with_acc(combined))
            return self._base_directive.agg(group_agg)
        
        return self._base_directive
    
    