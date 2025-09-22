import json
from bson import ObjectId
from loom.info import Collapsible, TimeInserted, StrUpper, StrLower, Model

# A concrete model for testing
class MyTestModel(Model):
    name: str
    value: int
    created_at: TimeInserted = None
    description: StrUpper = ""
    notes: StrLower = ""

def test_model_creation():
    """Tests that a model is created with no id."""
    m = MyTestModel(name="test", value=1)
    assert m.id is None
    assert m.is_entangled() is False

def test_collapse_id_sets_id():
    """Tests that collapse_id generates and sets the id."""
    m = MyTestModel(name="test", value=1)
    assert m.id is None
    
    # First collapse
    oid = m.collapse_id()
    assert isinstance(oid, ObjectId)
    assert m.id == oid
    assert m.is_entangled() is True
    
    # Second collapse should return the same id
    assert m.collapse_id() == oid

def test_model_creation_with_specific_id():
    """Tests creating a model with a specific ObjectId."""
    oid = ObjectId()
    m = MyTestModel(name="test", value=1, _id=oid)
    assert m.id == oid
    assert m.is_entangled() is True

def test_superpos_date_is_none_on_creation():
    """Tests that a SuperposDate field is None on creation."""
    m = MyTestModel(name="test", value=1)
    assert m.created_at is None

def test_dump_doc_aliases_id():
    """Tests that dump_doc correctly aliases 'id' to '_id'."""
    m = MyTestModel(name="test", value=1)
    assert "_id" not in m.dump_doc() # No id yet
    
    m.collapse_id() # Ensure id exists
    doc = m.dump_doc()
    assert "_id" in doc
    assert "id" not in doc
    assert doc["_id"] == m.id

def test_dump_json_serializes_objectid():
    """Tests that dump_json correctly serializes ObjectId to a string."""
    m = MyTestModel(name="test", value=1)
    m.collapse_id()
    
    json_output = m.dump_json()
    
    # Parse the JSON to check the type
    data = json.loads(json_output)
    
    assert "_id" in data
    assert isinstance(data["_id"], str)
    assert data["_id"] == str(m.id)

def test_queryable_transformer():
    """Tests the StrUpper annotation."""
    m = MyTestModel(name="test", value=1, description="hello world")
    assert m.description == "HELLO WORLD"

def test_str_lower_transformer():
    """Tests the StrLower annotation."""
    m = MyTestModel(name="test", value=1, notes="HELLO WORLD")
    assert m.notes == "hello world"

def test_get_fields_with_metadata():
    """Tests get_fields_with_metadata method."""
    hints = MyTestModel.get_fields_with_metadata(Collapsible)
    assert "id" not in hints
    assert "created_at" in hints
    assert "name" not in hints

def test_get_field_metadata():
    """Tests get_field_metadata method."""
    
    name_hints = MyTestModel.get_field_metadata("name", Collapsible)
    assert len(name_hints) == 0

    non_existent_hints = MyTestModel.get_field_metadata("non_existent_field")
    assert len(non_existent_hints) == 0