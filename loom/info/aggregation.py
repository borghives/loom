"""
A module for building MongoDB aggregation pipelines.

This module provides the `Aggregation` class, a fluent builder for constructing
complex MongoDB aggregation pipelines in a readable and chainable way.

Example:
    from store.query import Filter, SortOp

    pipeline = (
        Aggregation()
        .Match(Filter.EQ("status", "A"))
        .Group({"_id": "$customer_id", "total": {"$sum": "$amount"}})
        .Sort(SortOp.DESC("total"))
        .Limit(10)
    )
    
    mongo_pipeline = pipeline.Pipeline()
"""
from typing import List
from loom.info.sort_op import SortOp
from loom.info.filter import Filter
from pyrsistent import pvector, PVector, PMap, freeze, thaw

class Aggregation:
    """
    A builder class for creating MongoDB aggregation pipelines.

    This class provides a set of methods that correspond to the different
    stages of a MongoDB aggregation pipeline. Each method returns a new,
    immutable `Aggregation` object, so that the methods can be chained 
    together to create a complex pipeline.

    Usage:
        agg = Aggregation().Match({"status": "active"}).Limit(100)
        pipeline_stages = agg.Pipeline()
    """

    stages: PVector[PMap]

    def __init__(self, pipeline = None):
        """
        Initializes a new `Aggregation` object.

        Args:
            pipeline : An optional initial list of
                pipeline stages.
        """
        if pipeline is None:
            self.stages = pvector()
            return
        
        if isinstance(pipeline, Aggregation):
            self.stages = pipeline.stages
            return

        self.stages = freeze(pipeline)
        assert isinstance(self.stages, PVector)
    
    def __iter__(self):
        """
        Allows iterating over the pipeline stages as key-value pairs.

        For each stage in the pipeline (which is a dictionary with one key,
        like `{"$match": {...}}`), this iterator yields a tuple of
        (stage_name, stage_definition).

        The stage definition is returned as a mutable dictionary, by applying
        `thaw` to the internal immutable stage data.

        Example:
            agg = Aggregation().Match({"a": 1})
            for name, definition in agg:
                print(f"Stage: {name}, Definition: {definition}")
                # Prints: Stage: $match, Definition: {'a': 1}
        """
        return iter([(k, thaw(v)) for stage in self.stages for k,v in stage.items()])


    def Match(self, filter: Filter | dict) -> "Aggregation":
        """
        Adds a `$match` stage to the pipeline.

        This filters the documents to pass only the documents that match the
        specified condition(s) to the next pipeline stage.

        Args:
            filter (Filter | dict): The filter to apply. Can be a `Filter`
                object or a dictionary representing the query.

        Returns:
            Aggregation: The `Aggregation` object for chaining.
        """
        if isinstance(filter, Filter):
            if (filter.is_empty()):
                return self

            expression = filter.express()
        else:
            expression = filter

        return Aggregation(self.stages.append(freeze({"$match": expression})))

    def Group(self, group: dict) -> "Aggregation":
        """
        Adds a `$group` stage to the pipeline.

        This groups input documents by a specified identifier expression and
        applies accumulator expressions to each group.

        Args:
            group (dict): The group specification.

        Returns:
            Aggregation: The `Aggregation` object for chaining.
        """
        return Aggregation(self.stages.append(freeze({"$group": group})))

    def ReplaceRoot(self, root: dict) -> "Aggregation":
        """
        Adds a `$replaceRoot` stage to the pipeline.

        Replaces the input document with the specified document.

        Args:
            root (dict): The replacement document expression.

        Returns:
            Aggregation: The `Aggregation` object for chaining.
        """
        return Aggregation(self.stages.append(freeze({"$replaceRoot": root})))

    def Project(self, project: dict) -> "Aggregation":
        """
        Adds a `$project` stage to the pipeline.

        Reshapes each document in the stream, such as by adding new fields or
        removing existing fields.

        Args:
            project (dict): The projection specification.

        Returns:
            Aggregation: The `Aggregation` object for chaining.
        """
        return Aggregation(self.stages.append(freeze({"$project": project})))

    def AddFields(self, fields: dict) -> "Aggregation":
        """
        Adds an `$addFields` stage to the pipeline.

        Adds new fields to documents. Similar to `$project`, but `$addFields`
        outputs documents that contain all existing fields from the input
        documents and the newly added fields.

        Args:
            fields (dict): A dictionary of fields to add, where keys are the
                new field names and values are their expressions.

        Returns:
            Aggregation: The `Aggregation` object for chaining.
        """
        return Aggregation(self.stages.append(freeze({"$addFields": fields})))

    def Sort(self, sort: SortOp) -> "Aggregation":
        """
        Adds a `$sort` stage to the pipeline.

        Sorts all input documents and returns them to the pipeline in sorted order.

        Args:
            sort (SortOp): The `SortOp` object specifying the sort order.

        Returns:
            Aggregation: The `Aggregation` object for chaining.
        """

        if sort.is_empty():
            return self
        
        return Aggregation(self.stages.append(freeze({"$sort": sort.express()})))

    def Limit(self, limit: int) -> "Aggregation":
        """
        Adds a `$limit` stage to the pipeline.

        Passes the first `n` documents unmodified to the pipeline where `n` is
        the specified limit.

        Args:
            limit (int): The maximum number of documents to pass.

        Returns:
            Aggregation: The `Aggregation` object for chaining.
        """
        if limit == 0:
            return self

        return Aggregation(self.stages.append(freeze({"$limit": limit})))

    def Skip(self, skip: int) -> "Aggregation":
        """
        Adds a `$skip` stage to the pipeline.

        Skips over the specified number of documents that pass into the stage
        and passes the remaining documents to the next stage.

        Args:
            skip (int): The number of documents to skip.

        Returns:
            Aggregation: The `Aggregation` object for chaining.
        """
        return Aggregation(self.stages.append(freeze({"$skip": skip})))

    def Unwind(self, path: str) -> "Aggregation":
        """
        Adds a `$unwind` stage to the pipeline.

        Deconstructs an array field from the input documents to output a
        document for each element.

        Args:
            path (str): The path to an array field to unwind (e.g., `"$items"`).

        Returns:
            Aggregation: The `Aggregation` object for chaining.
        """
        return Aggregation(self.stages.append(freeze({"$unwind": path})))

    def Lookup(
        self, foreignCollection: str, localField: str, foreignField: str, toField: str = "result"
    ) -> "Aggregation":
        """
        Adds a `$lookup` stage to the pipeline for a simple equality match.

        Performs a left outer join to an unsharded collection in the same
        database to filter in documents from the "joined" collection for
        processing.

        Args:
            foreignCollection (str): The target collection to join with.
            localField (str): The field from the input documents.
            foreignField (str): The field from the documents of the "from" collection.
            toField (str, optional): The name for the new array field to add
                to the input documents. Defaults to `"result"`.

        Returns:
            Aggregation: The `Aggregation` object for chaining.
        """
        return Aggregation(self.stages.append(freeze(
            {
                "$lookup": {
                    "from": foreignCollection,
                    "localField": localField,
                    "foreignField": foreignField,
                    "as": toField,
                }
            }
        )))

    def Merge(self, merge: dict) -> "Aggregation":
        """
        Adds a `$merge` stage to the pipeline.

        Writes the results of the aggregation pipeline to a specified collection.
        The `$merge` stage must be the last stage in the pipeline.

        Args:
            merge (dict): The merge specification.

        Returns:
            Aggregation: The `Aggregation` object for chaining.
        """
        return Aggregation(self.stages.append(freeze({"$merge": merge})))

    def Out(self, coll: str) -> "Aggregation":
        """
        Adds an `$out` stage to the pipeline.

        Takes the documents returned by the aggregation pipeline and writes
        them to a specified collection. This must be the last stage.

        Args:
            coll (str): The name of the output collection.

        Returns:
            Aggregation: The `Aggregation` object for chaining.
        """
        return Aggregation(self.stages.append(freeze({"$out": coll})))

    def Sample(self, size: int) -> "Aggregation":
        """
        Adds a `$sample` stage to the pipeline.

        Randomly selects the specified number of documents from its input.

        Args:
            size (int): The number of documents to sample.

        Returns:
            Aggregation: The `Aggregation` object for chaining.
        """
        if (size == 0):
            return self

        return Aggregation(self.stages.append(freeze({"$sample": {"size": size}})))

    def GraphLookup(self, fields: dict) -> "Aggregation":
        """
        Adds a `$graphLookup` stage to the pipeline.

        Performs a recursive search on a collection, with options for restricting
        the search by depth and query.

        Args:
            fields (dict): A dictionary specifying the graph lookup options.
                See MongoDB documentation for `$graphLookup` for details.

        Returns:
            Aggregation: The `Aggregation` object for chaining.
        """
        return Aggregation(self.stages.append(freeze({"$graphLookup": fields})))

    def __or__(self, agg: "Aggregation") -> "Aggregation":
        """
        Merges this aggregation pipeline with another one.

        This allows combining two pipelines using the `|` operator, creating a
        new `Aggregation` object containing the stages from both.

        Args:
            agg (Aggregation): The `Aggregation` object to merge with.

        Returns:
            Aggregation: A new `Aggregation` object with the combined stages.
        
        Example:
            agg1 = Aggregation().Match({"a": 1})
            agg2 = Aggregation().Limit(10)
            combined_agg = agg1 | agg2
        """
        return Aggregation(self.stages + agg.stages)

    def Pipeline(self) -> List:
        """
        Returns the complete aggregation pipeline as a list of stages.

        This list is in the format expected by MongoDB drivers.

        Returns:
            List[dict]: The pipeline as a list of stage dictionaries.
        """
        return thaw(self.stages)