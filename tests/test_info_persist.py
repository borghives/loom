import unittest
from unittest.mock import MagicMock, patch
from bson import ObjectId
import pandas as pd
from datetime import datetime, timezone

from pydantic import Field
from pymongo import UpdateOne
from pymongo.errors import BulkWriteError

from loom.info.atomic import IncrCounter
from loom.info.persist import Persistable, declare_persist_db
from loom.info.model import CoalesceOnInsert


@declare_persist_db(db_name="test_db", collection_name="test_collection", version=1, test=True)
class TestModel(Persistable):
    name: str
    value: int

@declare_persist_db(collection_name="test_inc_collection", db_name="test_db", test=True)
class TestIncModel(Persistable):
    test_field: str
    counter: IncrCounter  = Field(description="An incrementing integer counter", default=0)
    counter2: IncrCounter = Field(description="An incrementing integer counter", default=0)

class PersistableTest(unittest.TestCase):

    def test_should_persist_property(self):
        """Test the should_persist property logic."""
        new_model = TestModel(name="new", value=1)
        self.assertTrue(new_model.should_persist)

        loaded_model = TestModel.from_db_doc({"_id": ObjectId(), "name": "loaded", "value": 2})
        self.assertFalse(loaded_model.should_persist)

        loaded_model.has_update = True
        self.assertTrue(loaded_model.should_persist)

    def test_get_set_instruction(self):
        """Test the get_set_instruction method."""

        model = TestModel(name="test", value=10)
        fixed_time = model.updated_time  
        assert fixed_time is not None

        set_instr, _ = model.get_set_instruction()
        self.assertIn("$set", set_instr)
        set_doc = set_instr["$set"]

        self.assertEqual(set_doc["name"], "test")
        self.assertEqual(set_doc["updated_time"], fixed_time)

    def test_get_update_instruction(self):
        """Test the complete update instruction generation."""
        fixed_time = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        
        created_at_transformer = TestModel.get_field_metadata("created_at", CoalesceOnInsert)[0]
        created_at_transformer.collapse = lambda : fixed_time

        model = TestModel(name="test", value=10)
        model.id = None
        model.created_at = None
        model.updated_time = None

        self.assertIsNotNone(model.updated_time)
        update_instr = model.get_update_instruction()

        self.assertIn("$set", update_instr)
        set_doc = update_instr["$set"]
        self.assertEqual(set_doc["updated_time"], model.updated_time)

        self.assertIn("$setOnInsert", update_instr)
        set_on_insert_doc = update_instr["$setOnInsert"]
        self.assertEqual(set_on_insert_doc["created_at"], fixed_time)

    def test_time_updated_field(self):
        """Test the behavior of the TimeUpdated field."""
        model = TestModel(name="test", value=10)
        self.assertIsInstance(model.updated_time, datetime)

        first_updated_time = model.updated_time
        model.updated_time = None
        self.assertIsInstance(model.updated_time, datetime)
        self.assertNotEqual(first_updated_time, model.updated_time)

        fixed_time = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        model.updated_time = fixed_time
        self.assertEqual(model.updated_time, fixed_time)

    def test_inc_op(self):
        # 1. Create and persist a new model with an increment
        model = TestIncModel(test_field="inc_test")

        print(f"Type of read_count: {type(model.counter)}")

        model.counter += 2
        model.counter2 += 3

        try:
            model.counter = 2
            assert False, "Expected AttributeError when setting counter directly"
        except AttributeError as e:
            print(f"Caught expected exception: {e}")

        assert model.counter == 2
        assert model.counter2 == 3

        print(f"Type of read_count: {type(model.counter)}")

        model.persist()

        assert model.counter == 2
        assert model.counter2 == 3
        model.persist()

        # 2. Check the database for the initial state
        collection = TestIncModel.get_db_collection()
        db_data = collection.find_one(model.self_filter().get_exp())
        assert db_data is not None
        assert db_data["test_field"] == "inc_test"
         # On first insert, then inc is 2. So it should be 2.
        assert db_data["counter"] == 2
        assert db_data["counter2"] == 3
        assert db_data["_id"] == model.id

        # 3. Load from DB, increment again, and persist
        assert model.id is not None
        loaded_model = TestIncModel.from_id(model.id)
        assert loaded_model is not None
        assert loaded_model.counter == 2

        loaded_model.counter += 3
        loaded_model.persist()

        # 4. Check the database for the updated state
        db_data_updated = collection.find_one(model.self_filter().get_exp())
        assert db_data_updated is not None
        # 2 + 3 = 5
        assert db_data_updated["counter"] == 5

        # Clean up
        collection.delete_one(model.self_filter().get_exp())


    @patch.object(TestModel, 'get_db_collection')
    def test_persist_instance(self, mock_get_collection):
        """Test persisting a single model instance."""
        mock_collection = MagicMock()
        mock_get_collection.return_value = mock_collection

        model = TestModel(name="to_persist", value=100)
        
        result_doc = {"_id": model.collapse_id(), "name": "persisted", "value": 101, "created_at": datetime.now(timezone.utc)}
        mock_collection.find_one_and_update.return_value = result_doc

        self.assertTrue(model.persist())

        mock_collection.find_one_and_update.assert_called_once()
        self.assertFalse(model.has_update)
        self.assertEqual(model.name, "persisted")

    @patch.object(TestModel, 'get_db_collection')
    def test_persist_lazy(self, mock_get_collection):
        """Test the lazy=True flag on the persist method."""
        mock_collection = MagicMock()
        mock_get_collection.return_value = mock_collection

        loaded_model = TestModel.from_db_doc({"_id": ObjectId(), "name": "loaded", "value": 1})
        self.assertFalse(loaded_model.persist(lazy=True))
        mock_collection.find_one_and_update.assert_not_called()

        new_model = TestModel(name="new", value=2)
        result_doc = {"_id": new_model.collapse_id(), "name": "new", "value": 2, "created_at": datetime.now(timezone.utc)}
        mock_collection.find_one_and_update.return_value = result_doc
        self.assertTrue(new_model.persist(lazy=True))
        self.assertEqual(mock_collection.find_one_and_update.call_count, 1)

    @patch.object(TestModel, 'get_db_collection')
    def test_persist_many(self, mock_get_collection):
        """Test persisting multiple model instances."""
        mock_collection = MagicMock()
        mock_get_collection.return_value = mock_collection

        items = [
            TestModel(name="item1", value=1),
            TestModel.from_db_doc({"_id": ObjectId(), "name": "item2", "value": 2}),
            TestModel.from_db_doc({"_id": ObjectId(), "name": "item3", "value": 3}),
        ]
        items[2].value = 33
        items[2].has_update = True

        TestModel.persist_many(items, lazy=True)

        mock_collection.bulk_write.assert_called_once()
        operations = mock_collection.bulk_write.call_args[0][0]
        self.assertEqual(len(operations), 2)
        self.assertIsInstance(operations[0], UpdateOne)

        self.assertFalse(items[0].has_update)
        self.assertFalse(items[2].has_update)

    @patch.object(TestModel, 'get_db_collection')
    def test_insert_dataframe(self, mock_get_collection):
        """Test inserting a pandas DataFrame."""
        mock_collection = MagicMock()
        mock_get_collection.return_value = mock_collection

        df = pd.DataFrame({"name": ["df_user1"], "value": [10], "updated_time": [None]})
        TestModel.insert_dataframe(df)

        mock_collection.insert_many.assert_called_once()
        inserted_records = mock_collection.insert_many.call_args[0][0]
        self.assertEqual(inserted_records[0]["name"], "df_user1")
        self.assertIn("updated_time", inserted_records[0])

    @patch.object(TestModel, 'get_db_collection')
    def test_insert_dataframe_ignores_duplicate_error(self, mock_get_collection):
        """Test that insert_dataframe handles and ignores duplicate key errors."""
        mock_collection = MagicMock()
        mock_get_collection.return_value = mock_collection
        
        error_details = {"writeErrors": [{"code": 11000}]}
        bwe = BulkWriteError(error_details)
        mock_collection.insert_many.side_effect = bwe

        df = pd.DataFrame({"name": ["test"], "value": [1]})
        
        try:
            TestModel.insert_dataframe(df)
        except BulkWriteError:
            self.fail("BulkWriteError with duplicate key was not ignored")


if __name__ == "__main__":
    unittest.main()