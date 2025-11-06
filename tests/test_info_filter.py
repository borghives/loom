from loom.info.filter import Filter
from loom.info.persistable import Persistable
import loom as lm
class TestModel(Persistable):
    name: str
    age: int
    a: int
    b: int
    c: int


def test_filter_and_simple():
    f1 = lm.fld("age") > 30
    f2 = lm.fld("name") == "John"
    combined = f1 & f2
    expected = {
        "$and": [
            {"age": {"$gt": 30}},
            {"name": "John"},
        ]
    }
    assert combined.express(TestModel.get_mql_driver()) == expected

def test_filter_and_same_field():
    f = (lm.fld("age")  > 30) & (lm.fld("age") < 40)
    expected = {
        "$and": [
            {"age": {"$gt": 30}},
            {"age": {"$lt": 40}},
        ]
    }
    assert f.express(TestModel.get_mql_driver()) == expected

def test_filter_or_simple():
    f = (lm.fld("age") < 18) | (lm.fld("age") > 65)
    expected = {
        "$or": [
            {"age": {"$lt": 18}},
            {"age": {"$gt": 65}},
        ]
    }
    assert f.express(TestModel.get_mql_driver()) == expected

def test_filter_and_flattening():
    f1 = lm.fld("a") == 1
    f2 = lm.fld("b") == 2
    f3 = lm.fld("c") == 3
    combined = (f1 & f2) & f3
    result = combined.express(TestModel.get_mql_driver())
    # The order of clauses in the flattened list should be deterministic.
    assert result == {
        "$and": [
            {"a": 1},
            {"b": 2},
            {"c": 3},
        ]
    }

def test_filter_or_flattening():
    f1 = lm.fld("a") == 1
    f2 = lm.fld("b") == 2
    f3 = lm.fld("c") == 3
    combined = (f1 | f2) | f3
    result = combined.express(TestModel.get_mql_driver())
    assert result == {
        "$or": [
            {"a": 1},
            {"b": 2},
            {"c": 3},
        ]
    }

def test_filter_mixed_and_or():
    # (a=1 or b=2) and c=3
    f = ((lm.fld("a") == 1) | (lm.fld("b") == 2)) & (lm.fld("c") == 3)
    expected = {
        "$and": [
            {"$or": [{"a": 1}, {"b": 2}]},
            {"c": 3},
        ]
    }
    assert f.express(TestModel.get_mql_driver()) == expected

def test_filter_mixed_or_and():
    # a=1 or (b=2 and c=3)
    f = (lm.fld("a") == 1) | ((lm.fld("b") == 2) & (lm.fld("c") == 3))
    expected = {
        "$or": [
            {"a": 1},
            {"$and": [{"b": 2}, {"c": 3}]},
        ]
    }
    assert f.express(TestModel.get_mql_driver()) == expected

def test_empty_filter_and():
    f1 = Filter()
    f2 = lm.fld("a") == 1
    assert (f1 & f2).express(TestModel.get_mql_driver()) == {"a": 1}
    assert (f2 & f1).express(TestModel.get_mql_driver()) == {"a": 1}

def test_empty_filter_or():
    f1 = Filter()
    f2 = lm.fld("a") == 1
    assert (f1 | f2).express(TestModel.get_mql_driver()) == {"a": 1}
    assert (f2 | f1).express(TestModel.get_mql_driver()) == {"a": 1}

def test_or_with_none():
    f1 = lm.fld("a") == 1
    combined = f1 | None
    assert combined.express(TestModel.get_mql_driver()) == {"a": 1}

def test_and_with_none():
    f1 = lm.fld("a") == 1
    combined = f1 & None
    assert combined.express(TestModel.get_mql_driver()) == {"a": 1}
