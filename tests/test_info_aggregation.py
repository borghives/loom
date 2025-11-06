import pytest
from loom.info.aggregation import Aggregation
from loom.info.directive import normalize_pipeline_stage
from loom.info.filter import Filter
from loom.info.sort_op import SortAsc, SortDesc

def test_aggregation_initialization():
    """Tests that the Aggregation builder initializes correctly."""
    agg = Aggregation()
    assert agg.pipeline() == []

    agg_with_pipeline = Aggregation([{"$match": {"a": 1}}])
    assert agg_with_pipeline.pipeline() == [{"$match": {"a": 1}}]

def test_match_stage():
    """Tests the match method."""
    agg = Aggregation().match(Filter({"status": "A"}))
    assert [normalize_pipeline_stage(stage)  for stage in agg.pipeline() if stage is not None ]== [{"$match": {"status": "A"}}]

    agg_dict = Aggregation().match({"status": "B"})
    assert [normalize_pipeline_stage(stage)  for stage in agg_dict.pipeline() if stage is not None ] == [{"$match": {"status": "B"}}]

def test_group_stage():
    """Tests the group method."""
    group_expr = {"_id": "$customer_id", "total": {"$sum": "$amount"}}
    agg = Aggregation().group(group_expr)
    assert [normalize_pipeline_stage(stage)  for stage in agg.pipeline() if stage is not None ] == [{"$group": group_expr}]

def test_sort_stage():
    """Tests the sort method."""
    agg = Aggregation().sort(SortDesc("total"))
    assert [normalize_pipeline_stage(stage)  for stage in agg.pipeline() if stage is not None ] == [{"$sort": {"total": -1}}]

    agg_asc = Aggregation().sort(SortAsc("name"))
    assert [normalize_pipeline_stage(stage)  for stage in agg_asc.pipeline() if stage is not None ] == [{"$sort": {"name": 1}}]

def test_limit_stage():
    """Tests the limit method."""
    agg = Aggregation().limit(10)
    assert agg.pipeline() == [{"$limit": 10}]

def test_skip_stage():
    """Tests the skip method."""
    agg = Aggregation().skip(5)
    assert agg.pipeline() == [{"$skip": 5}]

def test_project_stage():
    """Tests the project method."""
    project_expr = {"name": 1, "_id": 0}
    agg = Aggregation().project(project_expr)
    assert agg.pipeline() == [{"$project": project_expr}]

def test_unwind_stage():
    """Tests the unwind method."""
    agg = Aggregation().unwind("$items")
    assert agg.pipeline() == [{"$unwind": "$items"}]

def test_chaining_methods():
    """Tests that the builder methods can be chained together."""
    agg = (
        Aggregation()
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
    
    assert [normalize_pipeline_stage(stage)  for stage in agg.pipeline() if stage is not None ] == expected_pipeline

def test_or_operator():
    """Tests the merging of two Aggregation objects using the | operator."""
    agg1 = Aggregation().match({"a": 1})
    agg2 = Aggregation().limit(10)
    
    combined = agg1 | agg2
    
    expected_pipeline = [
        {"$match": {"a": 1}},
        {"$limit": 10},
    ]
    
    assert combined.pipeline() == expected_pipeline
