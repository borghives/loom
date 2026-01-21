import unittest
from datetime import datetime
from bson import ObjectId
from loom.info import Persistable, declare_persist_db
from loom.info.model import TimeInserted, TimeUpdated
import loom as lm

@declare_persist_db(db_name="test_db", collection_name="test_update_collection", version=1, test=True)
class TestUpdateModel(Persistable):
    name: str
    value: int
    created_at: TimeInserted
    updated_time: TimeUpdated

class TestPersistableUpdate(unittest.TestCase):
    def setUp(self):
        self.collection = TestUpdateModel.get_init_collection()
        self.collection.delete_many({})

    def tearDown(self):
        self.collection.delete_many({})

    def test_persist_updates_local_object(self):
        """
        Verify that persist() updates the local object with created_at, updated_time, and version.
        This relies on the return_document=ReturnDocument.AFTER behavior which returns the full doc.
        """
        model = TestUpdateModel(name="test_persist", value=1)
        self.assertIsNone(model.created_at)
        self.assertIsNone(model.updated_time)

        model.persist()

        self.assertIsNotNone(model.created_at)
        self.assertIsNotNone(model.updated_time)

if __name__ == "__main__":
    unittest.main()
