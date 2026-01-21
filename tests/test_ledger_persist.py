import unittest
from bson import ObjectId
from loom.info import LedgerModel, declare_persist_db, TimeUpdated
from pydantic import Field
import loom as lm

@declare_persist_db(db_name="test_db", collection_name="test_ledger_collection", version=1, test=True)
class TestLedgerModel(LedgerModel):
    name: str
    value: int
    updated_time: TimeUpdated = Field(
        description="Timestamp of series", default=None
    )

class LedgerPersistTest(unittest.TestCase):
    def setUp(self):
        self.collection = TestLedgerModel.get_init_collection()
        self.collection.delete_many({})

    def tearDown(self):
        self.collection.delete_many({})

    def test_persist_append_only(self):
        """Test that LedgerModel.persist() creates new documents (append-only)."""
        model = TestLedgerModel(name="entry", value=1)
        
        # First persist
        self.assertTrue(model.persist())
        first_id = model.id
        self.assertIsNotNone(first_id)
        
        # Verify first document exists
        doc1 = self.collection.find_one({"_id": first_id})
        self.assertIsNotNone(doc1)
        self.assertEqual(doc1["value"], 1)

        # Modify and persist again
        model.value = 2
        self.assertTrue(model.persist())
        second_id = model.id
        self.assertIsNotNone(second_id)
        
        # Verify IDs are different
        self.assertNotEqual(first_id, second_id)

        # Verify second document exists
        doc2 = self.collection.find_one({"_id": second_id})
        self.assertIsNotNone(doc2)
        self.assertEqual(doc2["value"], 2)

        # Verify first document is unchanged
        doc1_again = self.collection.find_one({"_id": first_id})
        self.assertEqual(doc1_again["value"], 1)
        
        # Verify total count is 2
        self.assertEqual(self.collection.count_documents({}), 2)

    def test_persist_many(self):
        """Test persisting multiple LedgerModel instances."""
        items = [
            TestLedgerModel(name="item1", value=10),
            TestLedgerModel(name="item2", value=20),
            TestLedgerModel(name="item3", value=30),
        ]
        
        TestLedgerModel.persist_many(items)
        
        self.assertEqual(self.collection.count_documents({}), 3)
        
        # Verify all items have IDs
        for item in items:
            self.assertIsNotNone(item.id)
            doc = self.collection.find_one({"_id": item.id})
            self.assertIsNotNone(doc)
            self.assertEqual(doc["name"], item.name)

    def test_persist_lazy(self):
        """Test lazy persistence for LedgerModel."""
        model = TestLedgerModel(name="lazy", value=100)
        
        # Should persist because it's new (has_update is True initially for new models?)
        # Let's check has_update logic. 
        # In PersistableBase, __init__ calls super().__init__ which is Pydantic's BaseModel.
        # It doesn't explicitly set _has_update to True, but accessing properties might?
        # Wait, let's check PersistableBase again.
        
        # Actually, let's just test the behavior.
        self.assertTrue(model.persist(lazy=True))
        self.assertEqual(self.collection.count_documents({}), 1)
        
        # Now has_update should be False (reset in persist)
        # Calling persist(lazy=True) again should return False and not insert
        self.assertFalse(model.persist(lazy=True))
        self.assertEqual(self.collection.count_documents({}), 1)
        
        # Update model
        model.value = 101
        # Now has_update should be True
        self.assertTrue(model.persist(lazy=True))
        self.assertEqual(self.collection.count_documents({}), 2)

if __name__ == "__main__":
    unittest.main()
