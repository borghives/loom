
from typing import Optional
from loom.info.model import StrUpper
from loom.info.persistable import Persistable
from pydantic import Field
import loom as lm
from loom.info.acc_op import DateToString
from loom.info.expression import FieldSpecification

class MyPersistence(Persistable):
    name: str
    age: int
    created_at: int # Simulating a timestamp field

def test_date_to_string_direct():
    """Tests the DateToString expression directly."""
    expr = DateToString(lm.fld("created_at"), "%Y-%m-%d")
    expected = {
        "$dateToString": {
            "format": "%Y-%m-%d",
            "date": "$created_at"
        }
    }
    # Using express() to resolve internal expressions
    assert expr.express(MyPersistence.get_mql_driver()) == expected

def test_date_to_string_with_timezone():
    """Tests the DateToString expression with timezone."""
    expr = DateToString(lm.fld("created_at"), "%Y-%m-%d", timezone="America/Los_Angeles")
    expected = {
        "$dateToString": {
            "format": "%Y-%m-%d",
            "date": "$created_at",
            "timezone": "America/Los_Angeles"
        }
    }
    assert expr.express(MyPersistence.get_mql_driver()) == expected

def test_queryable_field_with_date_string():
    """Tests QueryableField.with_date_string shorthand."""
    field_spec = lm.fld("formatted_date").with_date_string("created_at", "%Y-%m-%d")
    
    expected = {
        "formatted_date": {
            "$dateToString": {
                "format": "%Y-%m-%d",
                "date": "$created_at"
            }
        }
    }
    assert field_spec.express(MyPersistence.get_mql_driver()) == expected

def test_load_directive_add_fields_multiple_inputs():
    """Tests LoadDirective.add_fields with multiple arguments (mixing dict and FieldSpecification)."""
    directive = MyPersistence.filter()
    
    # Using the new varargs signature
    directive.add_fields(
        {"field1": 1},
        lm.fld("field2").with_(2),
        {"field3": 3}
    )
    
    pipeline = directive.get_pipeline_expr()
    
    # Expecting a single $addFields stage merging all inputs
    expected_stage = {
        "$addFields": {
            "field1": 1,
            "field2": 2,
            "field3": 3
        }
    }
    
    assert len(pipeline) == 1
    assert pipeline[0] == expected_stage

def test_load_directive_project_multiple_inputs():
    """Tests LoadDirective.project with multiple arguments."""
    directive = MyPersistence.filter()
    
    directive.project(
        lm.fld("name").with_(lm.pth("name")),
        {"age": 1}
    )
    
    pipeline = directive.get_pipeline_expr()
    
    expected_stage = {
        "$project": {
            "name": "$name",
            "age": 1
        }
    }
    
    assert len(pipeline) == 1
    assert pipeline[0] == expected_stage

def test_combine_field_specifications_logic():
    """Implicitly tested via the directive tests, but ensuring expected behavior for overwrites."""
    directive = MyPersistence.filter()
    
    # Later arguments should overwrite earlier ones if keys collide
    directive.add_fields(
        {"a": 1},
        {"a": 2}
    )
    
    pipeline = directive.get_pipeline_expr()
    
    expected_stage = {
        "$addFields": {
            "a": 2
        }
    }
    
    assert pipeline[0] == expected_stage
