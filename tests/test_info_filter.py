from loom.info.filter import Filter
from loom.info.field import fld


def test_filter_and_simple():
    f1 = fld("age") > 30
    f2 = fld("name") == "John"
    combined = f1 & f2
    expected = {
        "$and": [
            {"age": {"$gt": 30}},
            {"name": "John"},
        ]
    }
    assert combined.express() == expected

def test_filter_and_same_field():
    f = (fld("age") > 30) & (fld("age") < 40)
    expected = {
        "$and": [
            {"age": {"$gt": 30}},
            {"age": {"$lt": 40}},
        ]
    }
    assert f.express() == expected

def test_filter_or_simple():
    f = (fld("age") < 18) | (fld("age") > 65)
    expected = {
        "$or": [
            {"age": {"$lt": 18}},
            {"age": {"$gt": 65}},
        ]
    }
    assert f.express() == expected

def test_filter_and_flattening():
    f1 = fld("a") == 1
    f2 = fld("b") == 2
    f3 = fld("c") == 3
    combined = (f1 & f2) & f3
    result = combined.express()
    # The order of clauses in the flattened list should be deterministic.
    assert result == {
        "$and": [
            {"a": 1},
            {"b": 2},
            {"c": 3},
        ]
    }

def test_filter_or_flattening():
    f1 = fld("a") == 1
    f2 = fld("b") == 2
    f3 = fld("c") == 3
    combined = (f1 | f2) | f3
    result = combined.express()
    assert result == {
        "$or": [
            {"a": 1},
            {"b": 2},
            {"c": 3},
        ]
    }

def test_filter_mixed_and_or():
    # (a=1 or b=2) and c=3
    f = ((fld("a") == 1) | (fld("b") == 2)) & (fld("c") == 3)
    expected = {
        "$and": [
            {"$or": [{"a": 1}, {"b": 2}]},
            {"c": 3},
        ]
    }
    assert f.express() == expected

def test_filter_mixed_or_and():
    # a=1 or (b=2 and c=3)
    f = (fld("a") == 1) | ((fld("b") == 2) & (fld("c") == 3))
    expected = {
        "$or": [
            {"a": 1},
            {"$and": [{"b": 2}, {"c": 3}]},
        ]
    }
    assert f.express() == expected

def test_empty_filter_and():
    f1 = Filter()
    f2 = fld("a") == 1
    assert (f1 & f2).express() == {"a": 1}
    assert (f2 & f1).express() == {"a": 1}

def test_empty_filter_or():
    f1 = Filter()
    f2 = fld("a") == 1
    assert (f1 | f2).express() == {"a": 1}
    assert (f2 | f1).express() == {"a": 1}
