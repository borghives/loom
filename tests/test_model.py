from bson import ObjectId
from loom.info.model import Model, SuperposDate, StrUpper, Superposition

# A concrete model for testing
class MyTestModel(Model):
    name: str
    value: int
    created_at: SuperposDate = None
    description: StrUpper = ""

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

def test_queryable_transformer():
    """Tests the StrUpper annotation."""
    m = MyTestModel(name="test", value=1, description="hello world")
    assert m.description == "HELLO WORLD"

def test_get_field_hints():
    """Tests get_field_hints method."""
    hints = MyTestModel.get_field_hints(Superposition)
    assert "id" in hints
    assert "created_at" in hints
    assert "name" not in hints
    assert isinstance(hints["id"][0], Superposition)

def test_get_field_hint():
    """Tests get_field_hint method."""
    id_hints = MyTestModel.get_field_hint("id")
    assert len(id_hints) > 0 # Pydantic adds other things
    
    superposition_hints = MyTestModel.get_field_hint("id", Superposition)
    assert len(superposition_hints) == 1
    assert isinstance(superposition_hints[0], Superposition)

    name_hints = MyTestModel.get_field_hint("name", Superposition)
    assert len(name_hints) == 0

    non_existent_hints = MyTestModel.get_field_hint("non_existent_field")
    assert len(non_existent_hints) == 0