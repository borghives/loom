import pytest
from loom.info.aggregation import AggregationStages
from loom.info.filter import QueryPredicates
from loom.info.sort_op import SortAsc, SortDesc

def test_aggregation_initialization():
    """Tests that the Aggregation builder initializes correctly."""
    agg = AggregationStages()
    assert agg.pipeline() == []

    agg_with_pipeline = AggregationStages([{"$match": {"a": 1}}])
    assert agg_with_pipeline.pipeline() == [{"$match": {"a": 1}}]

def test_match_stage():
    """Tests the match method."""
    agg = AggregationStages().match(QueryPredicates({"status": "A"}))
    assert agg.express() == [{"$match": {"status": "A"}}]

    agg_dict = AggregationStages().match({"status": "B"})
    assert agg_dict.express() == [{"$match": {"status": "B"}}]

def test_group_stage():
    """Tests the group method."""
    group_expr = {"_id": "$customer_id", "total": {"$sum": "$amount"}}
    agg = AggregationStages().group(group_expr)
    assert agg.express() == [{"$group": group_expr}]

def test_sort_stage():
    """Tests the sort method."""
    agg = AggregationStages().sort(SortDesc("total"))
    assert agg.express() == [{"$sort": {"total": -1}}]

    agg_asc = AggregationStages().sort(SortAsc("name"))
    assert agg_asc.express() == [{"$sort": {"name": 1}}]

def test_limit_stage():
    """Tests the limit method."""
    agg = AggregationStages().limit(10)
    assert agg.pipeline() == [{"$limit": 10}]

def test_skip_stage():
    """Tests the skip method."""
    agg = AggregationStages().skip(5)
    assert agg.pipeline() == [{"$skip": 5}]

def test_project_stage():
    """Tests the project method."""
    project_expr = {"name": 1, "_id": 0}
    agg = AggregationStages().project(project_expr)
    assert agg.pipeline() == [{"$project": project_expr}]

def test_unwind_stage():
    """Tests the unwind method."""
    agg = AggregationStages().unwind("$items")
    assert agg.pipeline() == [{"$unwind": "$items"}]

def test_chaining_methods():
    """Tests that the builder methods can be chained together."""
    agg = (
        AggregationStages()
        .match({"status": "A"})
        .group({"_id": "$customer_id", "total": {"$sum": "$amount"}})
        .sort(SortDesc("total"))
        .limit(10)
    )
    
    expected_pipeline = [
        {"$match": {"status": "A"}},
        {"$group": {"_id": "$customer_id", "total": {"$sum": "$amount"}}},
        {"$sort": {"total": -1}},
        {"$limit": 10},
    ]
    
    assert agg.express() == expected_pipeline

def test_or_operator():
    """Tests the merging of two Aggregation objects using the | operator."""
    agg1 = AggregationStages().match({"a": 1})
    agg2 = AggregationStages().limit(10)
    
    combined = agg1 | agg2
    
    expected_pipeline = [
        {"$match": {"a": 1}},
        {"$limit": 10},
    ]
    
    assert combined.pipeline() == expected_pipeline
