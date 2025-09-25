from loom.info.model import Model
from loom.info.field import StrUpper

class MyTestModel(Model):
    name: StrUpper
    age: int

fld = MyTestModel.fields()


def test_parse_filter_simple():
    """Tests normalization on a simple {field: value} filter."""
    f = fld["name"] == "john"
    assert f.express() == {"name": "JOHN"}

def test_parse_filter_and():
    """Tests normalization within an $and clause."""
    f = (fld["name"] == "john") & (fld["age"] > 30)

    expected = {
        "$and": [
            {"name": "JOHN"},
            {"age": {"$gt": 30}},
        ]
    }
    assert f.express() == expected

def test_parse_filter_or():
    """Tests normalization within an $or clause."""
    f = (fld["name"] == "jane") | (fld["name"] == "jake")
    
    expected = {
        "$or": [
            {"name": "JANE"},
            {"name": "JAKE"},
        ]
    }
    assert f.express() == expected

def test_parse_filter_nested():
    """Tests normalization within a nested logical clause."""
    f = (fld["age"] < 20) & ((fld["name"] == "john") | (fld["name"] == "jane"))
    parsed_f = f.express()
    assert isinstance(parsed_f, dict)

    # Note: The exact order of the outer $and clauses may vary based on evaluation order,
    # so we check the components.
    assert "$and" in parsed_f
    assert len(parsed_f["$and"]) == 2
    assert {"age": {"$lt": 20}} in parsed_f["$and"]
    assert {"$or": [{"name": "JOHN"}, {"name": "JANE"}]} in parsed_f["$and"]

def test_parse_filter_with_operator():
    """Tests normalization on a field with an operator like $in."""
    f = fld["name"].is_in(["john", "jane"])
    parsed = f.express()
    
    expected = {
        "name": {"$in": ["JOHN", "JANE"]}
    }
    assert parsed == expected

def test_parse_filter_no_match():
    """Tests that no changes are made when no fields need normalization."""
    f = fld["age"] == 30
    parsed = f.express()
    
    assert parsed == {"age": 30}
