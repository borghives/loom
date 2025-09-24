from typing import Annotated
from loom.info.model import Model
from loom.info.field import fld, NormalizeQueryInput
from loom.info.directive import parse_filter

# Define a simple model with a field that uses NormalizeQueryInput for uppercasing.
StrUpper = Annotated[str, NormalizeQueryInput(str.upper)]

class MyTestModel(Model):
    name: StrUpper
    age: int


def test_parse_filter_simple():
    """Tests normalization on a simple {field: value} filter."""
    normalized_query_map = MyTestModel.get_fields_with_metadata(NormalizeQueryInput)
    
    f = fld("name") == "john"
    parsed = parse_filter(f, normalized_query_map)
    
    assert parsed == {"name": "JOHN"}

def test_parse_filter_and():
    """Tests normalization within an $and clause."""
    normalized_query_map = MyTestModel.get_fields_with_metadata(NormalizeQueryInput)
    
    f = (fld("name") == "john") & (fld("age") > 30)
    parsed = parse_filter(f, normalized_query_map)
    
    expected = {
        "$and": [
            {"name": "JOHN"},
            {"age": {"$gt": 30}},
        ]
    }
    assert parsed == expected

def test_parse_filter_or():
    """Tests normalization within an $or clause."""
    normalized_query_map = MyTestModel.get_fields_with_metadata(NormalizeQueryInput)
    
    f = (fld("name") == "jane") | (fld("name") == "jake")
    parsed = parse_filter(f, normalized_query_map)
    
    expected = {
        "$or": [
            {"name": "JANE"},
            {"name": "JAKE"},
        ]
    }
    assert parsed == expected

def test_parse_filter_nested():
    """Tests normalization within a nested logical clause."""
    normalized_query_map = MyTestModel.get_fields_with_metadata(NormalizeQueryInput)
    
    f = (fld("age") < 20) & ((fld("name") == "john") | (fld("name") == "jane"))
    parsed_f = parse_filter(f, normalized_query_map)
    assert isinstance(parsed_f, dict)

    # Note: The exact order of the outer $and clauses may vary based on evaluation order,
    # so we check the components.
    assert "$and" in parsed_f
    assert len(parsed_f["$and"]) == 2
    assert {"age": {"$lt": 20}} in parsed_f["$and"]
    assert {"$or": [{"name": "JOHN"}, {"name": "JANE"}]} in parsed_f["$and"]

def test_parse_filter_with_operator():
    """Tests normalization on a field with an operator like $in."""
    normalized_query_map = MyTestModel.get_fields_with_metadata(NormalizeQueryInput)
    
    f = fld("name").is_in(["john", "jane"])
    parsed = parse_filter(f, normalized_query_map)
    
    expected = {
        "name": {"$in": ["JOHN", "JANE"]}
    }
    assert parsed == expected

def test_parse_filter_no_match():
    """Tests that no changes are made when no fields need normalization."""
    normalized_query_map = MyTestModel.get_fields_with_metadata(NormalizeQueryInput)
    
    f = fld("age") == 30
    parsed = parse_filter(f, normalized_query_map)
    
    assert parsed == {"age": 30}
