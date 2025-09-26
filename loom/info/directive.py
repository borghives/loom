from typing import Optional, Type
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
from loom.info.sort_op import SortDesc, SortOp
from loom.info.persistable import Persistable

class LoadDirective[T: Persistable]:
    """
    A directive for loading data from a `Persistable` model.

    This class provides a fluent API for building queries to load data from a MongoDB collection.
    It supports filtering, sorting, limiting, and aggregation.
    """
    def __init__(self, persistable: Type[T]) -> None:
        self._aggregation_expr: Aggregation = Aggregation()
        self._persist_cls: Type[T] = persistable

    def filter(self, filter: Filter) -> "LoadDirective[T]":
        """
        Adds a filter to the query.

        Args:
            filter (Filter): The filter to add.

        Returns:
            LoadDirective: The `LoadDirective` object for chaining.
        """
        self._aggregation_expr = self._aggregation_expr.match(filter)
        return self

    def sort(self, sort: SortOp) -> "LoadDirective[T]":
        """
        Adds a sort to the query.

        Args:
            sort (SortOp): The sort to add.

        Returns:
            LoadDirective: The `LoadDirective` object for chaining.
        """
        self._aggregation_expr = self._aggregation_expr.sort(sort)
        return self
    
    def skip(self, skip: int) -> "LoadDirective[T]":
        """
        Adds a skip to the query.

        Args:
            skip (int): The number of documents to skip.

        Returns:
            LoadDirective: The `LoadDirective` object for chaining.
        """
        self._aggregation_expr = self._aggregation_expr.skip(skip)
        return self
    
    def limit(self, limit: int) -> "LoadDirective[T]":
        """
        Adds a limit to the query.

        Args:
            limit (int): The limit to add.

        Returns:
            LoadDirective: The `LoadDirective` object for chaining.
        """
        self._aggregation_expr = self._aggregation_expr.limit(limit)
        return self
    
    def sample(self, sample: int) -> "LoadDirective[T]":
        """
        Adds a sample stage to the aggregation pipeline.

        Args:
            sample (int): The number of documents to sample.

        Returns:
            LoadDirective: The `LoadDirective` object for chaining.
        """
        self._aggregation_expr = self._aggregation_expr.sample(sample)
        return self
    
    def aggregation(self, aggregation: Aggregation) -> "LoadDirective[T]":
        """
        Adds an aggregation pipeline to the query.

        Args:
            aggregation (Aggregation): The aggregation pipeline to add.

        Returns:
            LoadDirective: The `LoadDirective` object for chaining.
        """
        self._aggregation_expr |= aggregation
        return self
    
    def exec_aggregate(self, post_agg: Optional[Aggregation] = None):
        """
        Performs an aggregation query on the model's collection.

        Args:
            post_agg (Aggregation, optional): The aggregation pipeline to
                append to the end just for this execution

        Returns:
            CommandCursor: A `pymongo` cursor to the results of the aggregation.
        """
        p_cls = self._persist_cls
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
    
    def exists(self) -> bool:
        """
        Checks if at least one document matching the filter exists.

        Returns:
            bool: `True` if a matching document exists, `False` otherwise.
        """
        docs = self.load_aggregate(Aggregation().limit(1))
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
        return aggregate_arrow_all(collection, pipeline=self.get_pipeline_expr(), schema=schema or p_cls.get_arrow_schema())
    
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
        p_cls = self._persist_cls
        collection = p_cls.get_db_collection()
        return aggregate_polars_all(collection, pipeline=self.get_pipeline_expr(), schema=schema or p_cls.get_arrow_schema())

    def get_pipeline_expr(self, post_agg: Optional[Aggregation] = None) -> list[dict]:
        return (self._aggregation_expr | post_agg).pipeline()


   # --- parsing field from persistence ---
def transform_query_value(value, transformer):
    """Recursively applies a transformer to the values within a query operator, leaving operators untouched."""
    if isinstance(value, dict):
        # e.g., {"$gt": 30} -> {"$gt": transformer(30)}
        return {op: transform_query_value(op_val, transformer) for op, op_val in value.items()}
    if isinstance(value, list):
        # e.g., {"$in": [1, 2]} -> {"$in": [transformer(1), transformer(2)]}
        return [transform_query_value(item, transformer) for item in value]
    else:
        # It's a literal value, apply the transformer
        return transformer(value)

def parse_filter_recursive(expression, normalized_query_map):
    """Recursively traverses a filter expression to apply value normalizations."""
    if not isinstance(expression, dict):
        return expression

    # Handle logical operators ($and, $or, etc.)
    for logical_op in ("$and", "$or", "$nor"):
        if logical_op in expression:
            expression[logical_op] = [parse_filter_recursive(sub, normalized_query_map) for sub in expression[logical_op]]
            return expression
    if "$not" in expression:
        expression["$not"] = parse_filter_recursive(expression["$not"], normalized_query_map)
        return expression

    # Handle field-level expressions
    output_expression = {}
    for field, value in expression.items():
        if field in normalized_query_map:
            transformers = normalized_query_map[field]
            new_value = value
            for t in transformers:
                new_value = transform_query_value(new_value, t)
            output_expression[field] = new_value
        else:
            output_expression[field] = value
    return output_expression