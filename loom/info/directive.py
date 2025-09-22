from typing import Optional, Type
from pymongoarrow.api import ( #type: ignore
    Schema,  Table,
    find_arrow_all, aggregate_arrow_all,
    find_pandas_all, aggregate_pandas_all,
    find_polars_all, aggregate_polars_all
) 

import pandas as pd
import polars as pl

from loom.info.aggregation import Aggregation
from loom.info.filter import Filter
from loom.info.sort_op import SortDesc, SortOp
from loom.info.model import NormalizeQueryInput
from loom.info.persistable import Persistable

class LoadDirective:
    """
    A directive for loading data from a `Persistable` model.

    This class provides a fluent API for building queries to load data from a MongoDB collection.
    It supports filtering, sorting, limiting, and aggregation.
    """
    def __init__(self, persistable: Type[Persistable]) -> None:
        self.filter_expr        : Filter    = Filter()
        self.sort_expr          : SortOp    = SortOp()
        self.limit_expr         : int       = 0
        self.aggregation_expr   : Optional[Aggregation] = None

        self.persist_cls        = persistable

    def filter(self, filter: Filter) -> "LoadDirective":
        """
        Adds a filter to the query.

        Args:
            filter (Filter): The filter to add.

        Returns:
            LoadDirective: The `LoadDirective` object for chaining.
        """
        if self.aggregation_expr is None:
            self.filter_expr &= filter
        else:
            self.aggregation_expr = self.aggregation_expr.Match(filter)
        
        return self

    def sort(self, sort: SortOp) -> "LoadDirective":
        """
        Adds a sort to the query.

        Args:
            sort (SortOp): The sort to add.

        Returns:
            LoadDirective: The `LoadDirective` object for chaining.
        """
        if self.aggregation_expr is None:
            self.sort_expr &= sort
        else:
            self.aggregation_expr = self.aggregation_expr.Sort(sort)

        return self
    
    def limit(self, limit: int) -> "LoadDirective":
        """
        Adds a limit to the query.

        Args:
            limit (int): The limit to add.

        Returns:
            LoadDirective: The `LoadDirective` object for chaining.
        """
        if self.aggregation_expr is None:
            self.limit_expr = limit
        else:
            self.aggregation_expr = self.aggregation_expr.Limit(limit)

        return self
    
    def sample(self, sample: int) -> "LoadDirective":
        """
        Adds a sample stage to the aggregation pipeline.

        Args:
            sample (int): The number of documents to sample.

        Returns:
            LoadDirective: The `LoadDirective` object for chaining.
        """
        self.aggregation(Aggregation().Sample(sample))
        return self
    
    def aggregation(self, aggregation: Aggregation) -> "LoadDirective":
        """
        Adds an aggregation pipeline to the query.

        Args:
            aggregation (Aggregation): The aggregation pipeline to add.

        Returns:
            LoadDirective: The `LoadDirective` object for chaining.
        """
        self.aggregation_expr = self.flatten_to_aggregation() | aggregation
        self.filter_expr = Filter()
        self.sort_expr = SortOp()
        self.limit_expr = 0
        
        return self
    
    def flatten_to_aggregation(self) -> Aggregation:
        """
        Flattens the filter, sort, and limit into an aggregation pipeline.

        Returns:
            Aggregation: The flattened aggregation pipeline.
        """
        aggregation = self.aggregation_expr
        if (aggregation is None):
            aggregation = Aggregation()
            
        aggregation = aggregation.Match(self.filter_expr).Sort(self.sort_expr).Limit(self.limit_expr)
        return aggregation
    
    def exec_aggregate(self, post_agg: Optional[Aggregation] = None):
        """
        Performs an aggregation query on the model's collection.

        Args:
            post_agg (Aggregation, optional): The aggregation pipeline to
                append to the end just for this execution

        Returns:
            CommandCursor: A `pymongo` cursor to the results of the aggregation.
        """
        p_cls = self.persist_cls
        return p_cls.get_db_collection().aggregate(self.get_pipeline_expr(post_agg))

    def load_aggregate(self, post_agg: Optional[Aggregation] = None):
        """
        Executes an aggregation and returns the results as a list of models.

        Args:
            post_agg (Aggregation, optional): The aggregation pipeline to
                append to the end just for this execution

        Returns:
            list[Self]: A list of model instances.
        """
        p_cls = self.persist_cls
        with self.exec_aggregate(post_agg) as cursors:
            return [p_cls.from_db_doc(doc) for doc in cursors]

    def load_one(self):
        """
        Loads a single document from the database.

        Returns:
            Optional[Persistable]: An instance of the model, or `None` if no document is found.
        """
        p_cls = self.persist_cls
        
        if self.aggregation_expr is None:
            doc = p_cls.get_db_collection().find_one(filter=self.get_filter_expr(), sort=self.sort_expr.get_tuples())
            return p_cls.from_db_doc(doc) if doc else None
        
        docs = self.load_aggregate(Aggregation().Limit(1))
        return docs[0] if len(docs) > 0 else None
    
    def load_many(self):
        """
        Loads multiple documents from the database.

        Returns:
            list[Persistable]: A list of loaded model instances.
        """
        p_cls = self.persist_cls

        if self.aggregation_expr is None:
            with p_cls.get_db_collection().find(
                filter=self.get_filter_expr(), sort=self.sort_expr.get_tuples(), limit=self.limit_expr
            ) as cursor:
                return [p_cls.from_db_doc(doc) for doc in cursor]
        
        return self.load_aggregate()
    
    def load_latest(self, sort: SortOp = SortDesc("updated_time")):
        """
        Loads the most recently updated document from the database.

        Args:
            sort (SortOp, optional): Sort order. Defaults to `updated_time` descending.

        Returns:
            Optional[Persistable]: An instance of the loaded document, or `None` if not found.
        """
        p_cls = self.persist_cls

        if self.aggregation_expr is None:
            doc = p_cls.get_db_collection().find_one(filter=self.get_filter_expr(), sort=(sort & self.sort_expr).get_tuples())
            return p_cls.from_db_doc(doc) if doc else None
        
        docs = self.load_aggregate(Aggregation().Sort(sort).Limit(1))
        return docs[0] if len(docs) > 0 else None
    
    def exists(self) -> bool:
        """
        Checks if at least one document matching the filter exists.

        Returns:
            bool: `True` if a matching document exists, `False` otherwise.
        """
        p_cls = self.persist_cls
        if self.aggregation_expr is None:
            doc = p_cls.get_db_collection().find_one(filter=self.get_filter_expr())
            return doc is not None
        
        docs = self.load_aggregate(Aggregation().Limit(1))
        return len(docs) > 0
    
    def load_table(self, schema: Optional[Schema] = None) -> Table:
        """
        Loads data from a query into a PyArrow Table.

        Args:
            schema (Schema, optional): The PyArrow schema to use.

        Returns:
            Table: A PyArrow Table containing the loaded data.
        """
        p_cls = self.persist_cls
        collection = p_cls.get_db_collection()
        if self.aggregation_expr is None:
            return find_arrow_all(collection, query=self.get_filter_expr(), schema=schema or p_cls.get_arrow_schema(), sort=self.sort_expr.get_tuples(), limit=self.limit_expr)

        return aggregate_arrow_all(collection, pipeline=self.get_pipeline_expr(), schema=schema or p_cls.get_arrow_schema())
    
    def load_dataframe(self, schema: Optional[Schema] = None) -> pd.DataFrame:
        """
        Loads data from a query into a pandas DataFrame.

        Args:
            schema (Schema, optional): The PyArrow schema to use.

        Returns:
            pd.DataFrame: A pandas DataFrame containing the loaded data.
        """
        p_cls = self.persist_cls
        collection = p_cls.get_db_collection()
        if self.aggregation_expr is None:
            return find_pandas_all(collection, query=self.get_filter_expr(), schema=schema or p_cls.get_arrow_schema(), sort=self.sort_expr.get_tuples(), limit=self.limit_expr)
        
        return aggregate_pandas_all(collection, pipeline=self.get_pipeline_expr(), schema=schema or p_cls.get_arrow_schema())
    
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

    def load_polars(self, schema: Optional[Schema] = None) -> pl.DataFrame | pl.Series:
        """
        Loads data from a query into a polars DataFrame.

        Args:
            schema (Schema, optional): The PyArrow schema to use.

        Returns:
            pl.DataFrame | pl.Series: A polars DataFrame or Series containing the loaded data.
        """
        p_cls = self.persist_cls
        collection = p_cls.get_db_collection()
        if self.aggregation_expr is None:
            return find_polars_all(collection, query=self.get_filter_expr(), schema=schema or p_cls.get_arrow_schema(), sort=self.sort_expr.get_tuples(), limit=self.limit_expr)
        
        return aggregate_polars_all(collection, pipeline=self.get_pipeline_expr(), schema=schema or p_cls.get_arrow_schema())

    
    # --- parsing field from persistence ---
    def parse_filter(self, filter: Filter | dict) -> dict:
        """
        Parses a `Filter` object or dict into a MongoDB query dictionary.

        This method also applies any `NormalizeQueryInput` annotations on the
        model's fields to transform corresponding values in the filter.

        Args:
            filter (Filter | dict): The filter expression to parse.

        Returns:
            dict: A MongoDB-compatible query dictionary.
        """
        p_cls = self.persist_cls

        normalized_query_map = p_cls.get_fields_with_metadata(NormalizeQueryInput)

        retval: dict = filter.express() if isinstance(filter, Filter) else filter
        if not normalized_query_map:
            return retval

        for key, normalize_transformers in normalized_query_map.items():
            if key in retval:
                for transformer in normalize_transformers:
                    original_value = retval[key]
                    retval[key] = transform_filter_value(original_value, transformer)

        return retval

    def parse_agg_stage(self, stage: str, expr) -> dict:
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
            return {"$match": self.parse_filter(expr)}

        return {stage: expr}

    def parse_agg_pipe(self, aggregation: Aggregation) -> list[dict]:
        """
        Parses an `Aggregation` object into a MongoDB aggregation pipeline.

        Args:
            aggregation (Aggregation): The `Aggregation` object to parse.

        Returns:
            list[dict]: A list of dictionaries representing the pipeline.
        """
        return [self.parse_agg_stage(stage, expr) for stage, expr in aggregation]    
    
    def get_filter_expr(self) -> dict:
        """
        Gets the filter expression for the query.

        Returns:
            dict: The filter expression.
        """
        return self.parse_filter(self.filter_expr)
    
    def get_pipeline_expr(self, post_agg: Optional[Aggregation] = None) -> list[dict]:
        """
        Gets the aggregation pipeline for the query.

        Args:
            post_agg (Aggregation, optional): The aggregation pipeline to
                append to the end just for this execution

        Returns:
            list[dict]: The aggregation pipeline.
        """
        aggregation = self.flatten_to_aggregation()
        if post_agg is not None:
            aggregation |=  post_agg

        return self.parse_agg_pipe(aggregation)

def transform_filter_value(original_value, transformer):
    if isinstance(original_value, list):
        return [transform_filter_value(v, transformer) for v in original_value]
    if isinstance(original_value, dict):
        return {k: transform_filter_value(v, transformer) for k, v in original_value.items()}
    else:
        return transformer(original_value)
    


    
