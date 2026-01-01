from loom.info.expression import combine_field_specifications
from loom.info.field import QueryableField
from enum import Enum
from typing import Generic, List, Optional, Type, Self
from pymongoarrow.api import ( #type: ignore
    Schema,  Table,
    aggregate_arrow_all,
    aggregate_pandas_all,
    aggregate_polars_all
) 

import pandas as pd
import polars as pl

from loom.info.aggregation import AggregationStages
from loom.info.expression import Expression, FieldName, FieldPath, GroupExpression, FieldSpecification
from loom.info.filter import QueryPredicates
from loom.info.sort_op import SortDesc, SortOp, SortAsc
from loom.info.persistable_type import PersistableType

class WhenMatchedAction(Enum):
    REPLACE = "replace"
    MERGE = "merge"
    FAIL = "fail"
    KEEP_EXISTING = "keepExisting"
    PIPELINE = "pipeline"
    
class WhenNotMatchedAction(Enum):
    INSERT = "insert"
    DISCARD = "discard"
    FAIL = "fail"

class LoadDirective(Generic[PersistableType]):
    """
    A directive for loading data from a `Persistable` model.

    This class provides a fluent API for building queries to load data from a MongoDB collection.
    It supports filtering, sorting, limiting, and aggregation.
    """
    def __init__(self, persistable: Type[PersistableType]) -> None:
        self._aggregation_expr: AggregationStages = AggregationStages()
        self._persist_cls: Type[PersistableType] = persistable

    def filter(self, filter: QueryPredicates) -> Self:
        """
        Adds a filter to the query.

        Args:
            filter (Filter): The filter to add.

        Returns:
            LoadDirective: The `LoadDirective` object for chaining.
        """
        self._aggregation_expr = self._aggregation_expr.match(filter)
        return self

    def sort(self, sort: Optional[SortOp | str], descending: bool = False) -> Self:
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
    
    def skip(self, skip: int) -> Self:
        """
        Adds a skip to the query.

        Args:
            skip (int): The number of documents to skip.

        Returns:
            LoadDirective: The `LoadDirective` object for chaining.
        """
        self._aggregation_expr = self._aggregation_expr.skip(skip)
        return self

    def limit(self, limit: int) -> Self:
        """
        Adds a limit to the query.

        Args:
            limit (int): The limit to add.

        Returns:
            LoadDirective: The `LoadDirective` object for chaining.
        """
        self._aggregation_expr = self._aggregation_expr.limit(limit)
        return self
    
    def sample(self, sample: int) -> Self:
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

    def add_fields(self, *specifications: FieldSpecification | dict) -> Self:
        """
        Adds an addFields stage to the aggregation pipeline.

        Args:
            specifications (FieldSpecification | dict): The fields to add.

        Returns:
            LoadDirective: The `LoadDirective` object for chaining.
        """

        combined = combine_field_specifications(*specifications)
        
        if combined is not None:
            self._aggregation_expr = self._aggregation_expr.add_fields(combined)

        return self

    def project(self, *specifications: FieldSpecification | dict) -> Self:
        combined = combine_field_specifications(*specifications)
        
        if combined is not None:
            self._aggregation_expr = self._aggregation_expr.project(combined)

        return self

    def group_by(self, *keys: str | FieldSpecification | None) -> "GroupDirective[PersistableType]":
        combined: FieldSpecification = FieldSpecification()
        for key in keys:
            if isinstance(key, str):
                combined |= QueryableField(key).with_(key)
            else:
                combined |= key
        
        return GroupDirective[PersistableType](self, combined)
    
    def agg(self, aggregation: AggregationStages) -> Self:
        """
        Adds an aggregation pipeline to the query.

        Args:
            aggregation (Aggregation): The aggregation pipeline to add.

        Returns:
            LoadDirective: The `LoadDirective` object for chaining.
        """
        self._aggregation_expr |= aggregation
        return self
    
    def exec_agg(self, post_agg: Optional[AggregationStages] = None):
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

    def load_agg(self, post_agg: Optional[AggregationStages] = None) -> list[PersistableType]:
        """
        Executes an aggregation and returns the results as a list of models.

        Args:
            post_agg (Aggregation, optional): The aggregation pipeline to
                append to the end just for this execution

        Returns:
            list[PersistableType]: A list of model instances.
        """
        p_cls = self._persist_cls
        with self.exec_agg(post_agg) as cursors:
            return [p_cls.from_doc(doc) for doc in cursors]
    
    def load_one(self) -> Optional[PersistableType]:
        """
        Loads a single document from the database.

        Returns:
            Optional[Persistable]: An instance of the model, or `None` if no document is found.
        """
        docs = self.load_agg(AggregationStages().limit(1))
        return docs[0] if len(docs) > 0 else None
    
    def load_many(self) -> list[PersistableType]:
        """
        Loads multiple documents from the database.

        Returns:
            list[PersistableType]: A list of loaded model instances.
        """
        return self.load_agg()
    
    def load_latest(self, sort: SortOp = SortDesc("updated_time")) -> Optional[PersistableType]:
        """
        Loads the most recently updated document from the database.

        Args:
            sort (SortOp, optional): Sort order. Defaults to `updated_time` descending.

        Returns:
            Optional[Persistable]: An instance of the loaded document, or `None` if not found.
        """
        docs = self.load_agg(AggregationStages().sort(sort).limit(1))
        return docs[0] if len(docs) > 0 else None
    
    def merge_into(self, collection_name: str, on: List[str], when_matched: WhenMatchedAction = WhenMatchedAction.REPLACE, when_not_matched: WhenNotMatchedAction = WhenNotMatchedAction.INSERT):
        self._aggregation_expr = self._aggregation_expr.merge({
            "into": collection_name,
            "on": on,
            "whenMatched": when_matched.value,
            "whenNotMatched": when_not_matched.value 
        })
        self.exec_agg()
    

    async def exec_agg_async(self, post_agg: Optional[AggregationStages] = None):
        p_cls = self._persist_cls
        collection = await p_cls.get_init_collection_async()
        return await collection.aggregate(self.get_pipeline_expr(post_agg))

    async def load_agg_async(self, post_agg: Optional[AggregationStages] = None):
        p_cls = self._persist_cls
        cursor = await self.exec_agg_async(post_agg)
        async with cursor:
            async for doc in cursor:
                yield p_cls.from_doc(doc)

    async def load_one_async(self) -> Optional[PersistableType]:
        async for doc in self.load_agg_async(AggregationStages().limit(1)):
            return doc
    
    async def load_many_async(self) -> list[PersistableType]:
        return [doc async for doc in self.load_agg_async()]

    async def load_latest_async(self, sort: SortOp = SortDesc("updated_time")):
        async for doc in self.load_agg_async(AggregationStages().sort(sort).limit(1)):
            return doc
    
    async def merge_into_async(self, collection_name: str, on: List[str], when_matched: WhenMatchedAction = WhenMatchedAction.REPLACE, when_not_matched: WhenNotMatchedAction = WhenNotMatchedAction.INSERT):
        self._aggregation_expr = self._aggregation_expr.merge({
            "into": collection_name,
            "on": on,
            "whenMatched": when_matched.value, #<replace|keepExisting|merge|fail|pipeline>, 
            "whenNotMatched": when_not_matched.value #<insert|discard|fail> 
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
        doc = await self.load_one_async()
        return doc is not None
    
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
        return aggregate_arrow_all(collection, pipeline=self.get_pipeline_expr(), schema=Schema(schema) if schema else None)
    
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
        return aggregate_pandas_all(collection, pipeline=self.get_pipeline_expr(), schema=Schema(schema) if schema else None)

    def load_polars(self, schema: Optional[dict] = None) -> pl.DataFrame | pl.Series:
        """
        Loads data from a query into a polars DataFrame.

        Args:
            schema (Schema, optional): The PyArrow schema to use.

        Returns:
            pl.DataFrame | pl.Series: A polars DataFrame or Series containing the loaded data.
        """
        p_cls = self._persist_cls
        collection = p_cls.get_init_collection()
        return aggregate_polars_all(collection, pipeline=self.get_pipeline_expr(), schema=Schema(schema) if schema else None)

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
        p_cls = self._persist_cls
        flattened_pipelines =pipelines.express(p_cls.get_mql_driver())
        assert isinstance(flattened_pipelines, list)
        return flattened_pipelines


class GroupDirective(Generic[PersistableType]):
    def __init__(self, base_directive: LoadDirective[PersistableType], key: Expression | None):
        self._base_directive = base_directive
        self.group_expression = GroupExpression(key)
        
    def acc(self : "GroupDirective[PersistableType]", *accumulators: FieldSpecification) -> LoadDirective[PersistableType]:
        if (accumulators is None or len(accumulators) == 0):
            return self._base_directive
        
        combined: Optional[FieldSpecification] = None
        for accumulator in accumulators:
            if combined is None:
                combined = accumulator
            else:
                combined |= accumulator
        
        if combined is not None:
            group_agg = AggregationStages().group(self.group_expression.with_acc(combined))
            return self._base_directive.agg(group_agg)
        
        return self._base_directive
    
    