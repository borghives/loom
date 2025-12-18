from typing import Optional

import rich

from loom.info.model import StrUpper
from loom.info.op import multiply
from loom.info.persistable import Persistable
from pydantic import Field
import loom as lm
class MyTestModel(Persistable):
    name: StrUpper
    age: int = Field(default=0)
    description: Optional[str] = None

class MyPersistence(Persistable):
    name: str
    age: int



def test_parse_filter_simple():
    """Tests normalization on a simple {field: value} filter."""
    f = lm.fld("name") == "john"
    assert f.express(MyTestModel.get_mql_driver()) == {"name": "JOHN"}

def test_parse_filter_and():
    """Tests normalization within an $and clause."""
    f = (lm.fld("name") == "john") & (lm.fld("age") > 30)

    expected = {
        "$and": [
            {"name": "JOHN"},
            {"age": {"$gt": 30}},
        ]
    }
    assert f.express(MyTestModel.get_mql_driver()) == expected

def test_parse_filter_or():
    """Tests normalization within an $or clause."""
    f = (lm.fld("name") == "jane") | (lm.fld("name") == "jake")
    
    expected = {
        "$or": [
            {"name": "JANE"},
            {"name": "JAKE"},
        ]
    }
    assert f.express(MyTestModel.get_mql_driver()) == expected

def test_parse_filter_nested():
    """Tests normalization within a nested logical clause."""
    f = (lm.fld("age") < 20) & ((lm.fld("name") == "john") | (lm.fld("name") == "jane"))
    parsed_f = f.express(MyTestModel.get_mql_driver())
    assert isinstance(parsed_f, dict)

    # Note: The exact order of the outer $and clauses may vary based on evaluation order,
    # so we check the components.
    assert "$and" in parsed_f
    assert len(parsed_f["$and"]) == 2
    assert {"age": {"$lt": 20}} in parsed_f["$and"]
    assert {"$or": [{"name": "JOHN"}, {"name": "JANE"}]} in parsed_f["$and"]

def test_parse_filter_with_operator():
    """Tests normalization on a field with an operator like $in."""
    f = lm.fld("name").is_in(["john", "jane"])
    parsed = f.express(MyTestModel.get_mql_driver())
    
    expected = {
        "name": {"$in": ["JOHN", "JANE"]}
    }
    assert parsed == expected

def test_parse_filter_no_match():
    """Tests that no changes are made when no fields need normalization."""
    f = lm.fld("age") == 30
    parsed = f.express(MyTestModel.get_mql_driver())
    
    assert parsed == {"age": 30}


def test_load_directive_skip():
    """Tests the skip method of the LoadDirective."""

    directive = MyPersistence.filter()
    directive.skip(10)
    
def test_load_directive_group_by():
    """Tests the group_by method of the LoadDirective."""

    directive = MyPersistence.filter()
    group_directive = directive.group_by("name").acc(
        lm.fld("median_age_of_name").with_median("age")
    )
    pipeline = group_directive.get_pipeline_expr()
    expected_pipeline =[
        {
            '$group': {
                '_id': {
                    'name': '$name'
                },
                'median_age_of_name': {
                    '$median': {'input': '$age', 'method': 'approximate'}
                }
            }
        }
    ]
    assert pipeline == expected_pipeline


def test_load_directive_group_by_sum():
    """Tests the group_by method of the LoadDirective with a sum accumulator."""

    directive = MyPersistence.filter()
    group_directive = directive.group_by("name").acc(
        lm.fld("total_age").with_sum("age")
    )
    pipeline = group_directive.get_pipeline_expr()
    expected_pipeline = [
        {
            '$group': {
                '_id':  {
                    'name': '$name'
                },
                'total_age': {
                    '$sum': '$age'
                }
            }
        }
    ]
    assert pipeline == expected_pipeline


def test_load_directive_group_by_avg():
    """Tests the group_by method of the LoadDirective with an avg accumulator."""

    directive = MyPersistence.filter()
    group_directive = directive.group_by("name").acc(
        lm.fld("avg_age").with_avg("age")
    )
    pipeline = group_directive.get_pipeline_expr()
    expected_pipeline = [
        {
            '$group': {
                '_id': {
                    'name': '$name'
                },
                'avg_age': {
                    '$avg': '$age'
                }
            }
        }
    ]
    assert pipeline == expected_pipeline


def test_load_directive_group_by_min():
    """Tests the group_by method of the LoadDirective with a min accumulator."""

    directive = MyPersistence.filter()
    group_directive = directive.group_by("name").acc(
        lm.fld("min_age").with_min("age")
    )
    pipeline = group_directive.get_pipeline_expr()
    expected_pipeline = [
        {
            '$group': {
                '_id': {
                    'name': '$name'
                },
                'min_age': {
                    '$min': '$age'
                }
            }
        }
    ]
    assert pipeline == expected_pipeline


def test_load_directive_group_by_max():
    """Tests the group_by method of the LoadDirective with a max accumulator."""

    directive = MyPersistence.filter()
    group_directive = directive.group_by("name").acc(
        lm.fld("max_age").with_max("age")
    )
    pipeline = group_directive.get_pipeline_expr()
    expected_pipeline = [
        {
            '$group': {
                '_id': {
                    'name': '$name'
                },
                'max_age': {
                    '$max': '$age'
                }
            }
        }
    ]
    assert pipeline == expected_pipeline


def test_load_directive_group_by_multiple_accumulators():
    """Tests the group_by method of the LoadDirective with multiple accumulators."""

    directive = MyPersistence.filter()
    group_directive = directive.group_by("name").acc(
        lm.fld("total_age").with_sum("age") |
        lm.fld("avg_age").with_avg("age")
    )
    pipeline = group_directive.get_pipeline_expr()
    expected_pipeline = [
        {
            '$group': {
                '_id': {
                    'name': '$name'
                },
                'total_age': {
                    '$sum': '$age'
                },
                'avg_age': {
                    '$avg': '$age'
                }
            }
        }
    ]
    assert pipeline == expected_pipeline


def test_load_directive_project():
    """Tests the project method of the LoadDirective."""

    directive = MyPersistence.filter()
    directive.project(
        lm.fld("new_name").with_(lm.pth("name"))
    )
    pipeline = directive.get_pipeline_expr()
    expected_pipeline = [
        {
            '$project': {
                'new_name': '$name'
            }
        }
    ]
    assert pipeline == expected_pipeline


def test_load_directive_project_dict():
    """Tests the project method of the LoadDirective with a dict."""

    directive = MyPersistence.filter()
    directive.project(
        {'new_name': '$name'}
    )
    pipeline = directive.get_pipeline_expr()
    expected_pipeline = [
        {
            '$project': {
                'new_name': '$name'
            }
        }
    ]
    assert pipeline == expected_pipeline

def test_load_directive_project_spec():
    """Tests the project method of the LoadDirective with a dict."""

    directive = MyPersistence.filter()
    directive.project(
        lm.fld("twice_age").with_(multiply(2, lm.pth("age")))
    )
    pipeline = directive.get_pipeline_expr()
    expected_pipeline = [{'$project': {'twice_age': {'$multiply': [2, '$age']}}}]
    rich.print(pipeline)
    assert pipeline == expected_pipeline