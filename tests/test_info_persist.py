import unittest
from datetime import datetime, timezone
from typing import cast
from unittest.mock import MagicMock, patch

import pandas as pd
import polars as pl
import rich
from bson import ObjectId
from pydantic import Field
from pymongo import UpdateOne
from pymongo.errors import BulkWriteError

import loom as lm
from loom.info import CoalesceOnInsert, RefreshOnSet, Persistable, IncrCounter, declare_persist_db
from loom.info.model import StrLower, StrUpper



@declare_persist_db(db_name="test_db", collection_name="test_collection", version=1, test=True)
class TestModel(Persistable):
    name: str
    value: int
    link_id: ObjectId | None = None


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
        fixed_time = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        
        # Directly modify the collapse function for the test
        updated_time_transformer = TestModel.get_field_metadata("updated_time", RefreshOnSet)[0]
        updated_time_transformer.refresh = lambda x: fixed_time

        model = TestModel(name="test", value=10)
        model.updated_time = None  # Ensure coalesce is triggered

        set_instr, _ = model.get_set_instruction()
        self.assertIn("$set", set_instr)
        set_doc = set_instr["$set"]

        self.assertEqual(set_doc["name"], "test")
        self.assertEqual(set_doc["updated_time"], fixed_time)

    def test_get_update_instruction(self):
        """Test the complete update instruction generation."""
        fixed_time = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        
        # Directly modify the refresh functions for the test
        updated_time_transformer = TestModel.get_field_metadata("updated_time", RefreshOnSet)[0]
        updated_time_transformer.refresh = lambda x: fixed_time
        created_at_transformer = TestModel.get_field_metadata("created_at", CoalesceOnInsert)[0]
        created_at_transformer.collapse = lambda : fixed_time

        model = TestModel(name="test", value=10)
        model.id = None
        model.created_at = None
        model.updated_time = None

        update_instr = model.get_update_instruction()

        self.assertIn("$set", update_instr)
        set_doc = update_instr["$set"]
        self.assertEqual(set_doc["updated_time"], fixed_time)

        self.assertIn("$setOnInsert", update_instr)
        set_on_insert_doc = update_instr["$setOnInsert"]
        self.assertEqual(set_on_insert_doc["created_at"], fixed_time)

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
        db_data = collection.find_one(model.self_filter())
        assert db_data is not None
        assert db_data["test_field"] == "inc_test"
         # On first insert, then inc is 2. So it should be 2.
        assert db_data["counter"] == 2
        assert db_data["counter2"] == 3
        assert db_data["_id"] == model.id

        # 3. Load from DB, increment again, and persist
        assert model.id is not None
        loaded_model = cast(TestIncModel, TestIncModel.from_id(model.id))
        assert loaded_model is not None
        assert loaded_model.counter == 2

        loaded_model.counter += 3
        loaded_model.persist()

        # 4. Check the database for the updated state
        db_data_updated = collection.find_one(model.self_filter())
        assert db_data_updated is not None
        # 2 + 3 = 5
        assert db_data_updated["counter"] == 5

        # Clean up
        collection.delete_one(model.self_filter())


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

    def test_load_dataframe(self):
        """Test loading data into a pandas DataFrame."""
        collection = TestModel.get_db_collection()
        collection.delete_many({})

        df = pd.DataFrame([
            {"name": "df_user1", "value": 11, "link_id": ObjectId()},
            {"name": "df_user2", "value": 20, "link_id": ObjectId()},
        ])
        TestModel.insert_dataframe(df)

        loaded_df = TestModel.filter().load_dataframe()

        self.assertEqual(len(loaded_df), 2)
        self.assertIn("name", loaded_df.columns)
        self.assertIn("value", loaded_df.columns)
        
        rich.print(loaded_df)

        # Clean up
        collection.delete_many({})

class TestLoadDirective(unittest.TestCase):
    def setUp(self):
        self.collection = TestModel.get_db_collection()
        self.collection.delete_many({})
        TestModel(name="Alice", value=30).persist()
        TestModel(name="Bob", value=40).persist()
        TestModel(name="Charlie", value=50).persist()

    def tearDown(self):
        self.collection.delete_many({})

    def test_load_one(self):
        user = TestModel.filter(lm.fld('name') == "Alice").load_one()
        self.assertIsNotNone(user)
        self.assertIsInstance(user, TestModel)
        assert user is not None
        self.assertEqual(user.name, "Alice")

    def test_load_many(self):
        users = TestModel.filter(lm.fld('value') > 35).load_many()
        self.assertEqual(len(users), 2)

    def test_load_latest(self):
        latest_user = cast(TestModel, TestModel.filter(lm.fld('name') == "Charlie").load_latest())
        self.assertIsNotNone(latest_user)
        self.assertEqual(latest_user.name, "Charlie")

    def test_exists(self):
        self.assertTrue(TestModel.filter(lm.fld('name') == "Alice").exists())
        self.assertFalse(TestModel.filter(lm.fld('name') == "David").exists())

    def test_load_dataframe(self):
        df = TestModel.filter().load_dataframe()
        self.assertEqual(len(df), 3)
        self.assertIn("name", df.columns)

    def test_load_polars(self):
        df = TestModel.filter().load_polars()
        self.assertEqual(len(df), 3)
        self.assertIsInstance(df, pl.DataFrame)
        assert isinstance(df, pl.DataFrame)
        self.assertIn("name", df.columns)

    def test_load_table(self):
        table = TestModel.filter().load_table()
        self.assertEqual(len(table), 3)
        self.assertIn("name", table.column_names)


@declare_persist_db(db_name="test_db", collection_name="test_norm_collection", test=True)
class TestNormModel(Persistable):
    name: str
    description: StrUpper
    notes: StrLower


class TestNormalizeQueryInput(unittest.TestCase):
    def setUp(self):
        self.collection = TestNormModel.get_db_collection()
        self.collection.delete_many({})
        TestNormModel(name="test", description="UPPER", notes="lower").persist()

    def tearDown(self):
        self.collection.delete_many({})

    def test_normalize_query_input_filter(self):
        # Query with un-normalized value
        item = TestNormModel.filter(lm.fld('description') == 'upper').load_one()
        self.assertIsNotNone(item)
        assert item is not None
        self.assertEqual(item.description, "UPPER")

        item = TestNormModel.filter(lm.fld('notes') == 'LOWER').load_one()
        self.assertIsNotNone(item)
        assert item is not None

        self.assertEqual(item.notes, "lower")

    def test_normalize_query_input_filter_in_op(self):
        # Query with un-normalized value in a list
        item = TestNormModel.filter(lm.fld('description').is_in(['upper', 'another'])).load_one()
        self.assertIsNotNone(item)
        assert item is not None

        self.assertEqual(item.description, "UPPER")

        item = TestNormModel.filter(lm.fld('notes').is_in(['LOWER', 'ANOTHER'])).load_one()
        self.assertIsNotNone(item)
        assert item is not None

        self.assertEqual(item.notes, "lower")


        self.assertEqual(item.notes, "lower")


class TestFindSimple(unittest.TestCase):
    def setUp(self):
        self.collection = TestModel.get_db_collection()
        self.collection.delete_many({})
        TestModel(name="Alice", value=30).persist()
        TestModel(name="Bob", value=40).persist()
        TestModel(name="Charlie", value=50).persist()

    def tearDown(self):
        self.collection.delete_many({})

    def test_find_simple_load_many(self):
        users = TestModel.find_simple(lm.fld('value') > 35).load_many()
        self.assertEqual(len(users), 2)
        self.assertCountEqual([user.name for user in users], ["Bob", "Charlie"])

    def test_find_simple_load_one(self):
        user = TestModel.find_simple(lm.fld('name') == "Alice").load_one()
        self.assertIsNotNone(user)
        assert user is not None
        self.assertEqual(user.name, "Alice")

    def test_find_simple_count(self):
        count = TestModel.find_simple(lm.fld('value') > 35).count()
        self.assertEqual(count, 2)


if __name__ == "__main__":
    unittest.main()