
from typing import Annotated
import unittest
from unittest.mock import MagicMock, patch, ANY
from bson import ObjectId
import pandas as pd
from datetime import datetime

from loom.info.persist import Persistable
from loom.info.model import CoalesceOnInsert, TimeInserted, TimeUpdated
from loom.info.filter import Filter, SortDesc
from loom.info.aggregation import Aggregation
from pydantic import Field


# A decorator to add db metadata for testing purposes, mimicking the real one.
def declare_persist_db(db_name, collection_name, remote_db=False, version=None, **kwargs):
    def decorator(cls):
        cls._db_metadata = {
            "db_name": db_name,
            "collection_name": collection_name,
            "remote_db": remote_db,
            "version": version,
        }
        return cls
    return decorator


@declare_persist_db(db_name="test_db", collection_name="test_collection", version=1)
class TestModel(Persistable):
    name: str
    value: int
    coalesce_field: Annotated[str | None, CoalesceOnInsert(lambda x: x.upper() if x is not None else "")] = None


class UnregisteredModel(Persistable):
    name: str


class PersistableTest(unittest.TestCase):
    def test_initialization_and_private_fields(self):
        """Test that a new model has updates and a loaded one does not."""
        # A newly created model should need persisting
        model = TestModel(name="test", value=1, coalesce_field="coalesce")
        self.assertTrue(model.has_update)

        # A model loaded from DB should not have updates pending
        doc = {"_id": ObjectId(), "name": "test", "value": 1, "coalesce_field": "coalesce"}
        model = TestModel.from_db_doc(doc)
        self.assertFalse(model.has_update)

    def test_has_update_property(self):
        """Test the getter and setter for has_update."""
        model = TestModel(name="test", value=1, coalesce_field="coalesce")
        self.assertTrue(model.has_update)
        model.has_update = False
        self.assertFalse(model.has_update)

    def test_self_filter(self):
        """Test that self_filter returns a correct filter for the model's _id."""
        obj_id = ObjectId()
        model = TestModel.from_db_doc({"_id": obj_id, "name": "test", "value": 1, "coalesce_field": "coalesce"})
        filter_exp = model.self_filter()
        self.assertEqual(filter_exp.get_exp(), {"_id": obj_id})

    def test_update_instructions(self):
        """Test the generation of $set and $setOnInsert instructions."""
        model = TestModel(name="test", value=10, coalesce_field="do_not_set")
        
        # Get the complete update instruction
        update_instr = model.get_update_instruction()

        # Check the structure
        self.assertIn("$set", update_instr)
        self.assertIn("$setOnInsert", update_instr)

        # Check the $set part
        self.assertEqual(update_instr["$set"], {"name": "test", "value": 10})

        # Check the $setOnInsert part
        set_on_insert = update_instr["$setOnInsert"]
        self.assertIn("version", set_on_insert)
        self.assertIn("coalesce_field", set_on_insert)
        self.assertIn("created_at", set_on_insert)
        self.assertEqual(set_on_insert["version"], 1)
        self.assertEqual(set_on_insert["coalesce_field"], "do_not_set")
        self.assertIsInstance(set_on_insert["created_at"], datetime)

    def test_db_info_methods(self):
        """Test retrieval of database metadata."""
        self.assertEqual(TestModel.get_db_name(), "test_db")
        self.assertEqual(TestModel.get_db_collection_name(), "test_collection")
        self.assertEqual(TestModel.get_model_code_version(), 1)
        self.assertFalse(TestModel.is_remote_db())

    def test_db_info_exceptions(self):
        """Test that exceptions are raised for unregistered models."""
        with self.assertRaisesRegex(Exception, "is not decorated with @declare_persist_db"):
            UnregisteredModel.get_db_info()
        
        with patch.object(UnregisteredModel, "_db_metadata", {}, create=True):
            with self.assertRaisesRegex(Exception, "does not have db_name defined"):
                UnregisteredModel.get_db_name()

    @patch('loom.info.persist.get_local_db_client')
    @patch('loom.info.persist.get_remote_db_client')
    def test_db_client_retrieval(self, mock_remote_client, mock_local_client):
        """Test that the correct DB client is returned."""
        TestModel.get_db_client()
        mock_local_client.assert_called_once()
        mock_remote_client.assert_not_called()

        # Change metadata to remote and test again
        with patch.object(TestModel, "_db_metadata", {"remote_db": True, "db_name": "test", "collection_name": "test"}):
            TestModel.get_db_client()
            mock_remote_client.assert_called_once()

    def test_aggregation_parsing(self):
        """Test the parsing of aggregation pipelines."""
        agg = Aggregation().Match(Filter({"name": "test"})).Sort(SortDesc("value"))
        parsed_pipe = TestModel.parse_agg_pipe(agg)
        expected_pipe = [
            {"$match": {"name": "test"}},
            {"$sort": {"value": -1}},
        ]
        self.assertEqual(parsed_pipe, expected_pipe)

    @patch.object(TestModel, 'get_db_collection')
    def test_load_one(self, mock_get_collection):
        """Test loading a single document."""
        mock_collection = MagicMock()
        mock_get_collection.return_value = mock_collection
        
        doc = {"_id": ObjectId(), "name": "test", "value": 1}
        mock_collection.find_one.return_value = doc

        model = TestModel.load_one(name="test")
        
        mock_collection.find_one.assert_called_once_with({"name": "test"}, sort=None)
        self.assertIsNotNone(model)
        self.assertEqual(model.name, "test")

    @patch.object(TestModel, 'get_db_collection')
    def test_from_id(self, mock_get_collection):
        """Test loading a document by its ObjectId."""
        mock_collection = MagicMock()
        mock_get_collection.return_value = mock_collection
        
        obj_id = ObjectId()
        doc = {"_id": obj_id, "name": "by_id", "value": 1}
        mock_collection.find_one.return_value = doc

        # Test with ObjectId
        model = TestModel.from_id(obj_id)
        mock_collection.find_one.assert_called_with({"_id": obj_id}, sort=None)
        self.assertEqual(model.name, "by_id")

        # Test with valid string
        model_str = TestModel.from_id(str(obj_id))
        mock_collection.find_one.assert_called_with({"_id": obj_id}, sort=None)
        self.assertEqual(model_str.name, "by_id")

        # Test with invalid string
        self.assertIsNone(TestModel.from_id("invalid-id"))

    @patch.object(TestModel, 'get_db_collection')
    def test_load_many(self, mock_get_collection):
        """Test loading multiple documents."""
        mock_collection = MagicMock()
        mock_get_collection.return_value = mock_collection
        
        docs = [
            {"_id": ObjectId(), "name": "test1", "value": 1},
            {"_id": ObjectId(), "name": "test2", "value": 2},
        ]
        mock_cursor = MagicMock()
        mock_cursor.__enter__.return_value = iter(docs)
        mock_collection.find.return_value = mock_cursor

        models = TestModel.load_many(value={"$gt": 0})
        
        mock_collection.find.assert_called_once_with({"value": {"$gt": 0}}, limit=0, sort=None)
        self.assertEqual(len(models), 2)
        self.assertEqual(models[0].name, "test1")

    @patch.object(TestModel, 'get_db_collection')
    def test_load_dataframe(self, mock_get_collection):
        """Test loading data into a pandas DataFrame."""
        mock_collection = MagicMock()
        mock_get_collection.return_value = mock_collection
        
        docs = [
            {"_id": ObjectId(), "name": "test1", "value": 1},
            {"_id": ObjectId(), "name": "test2", "value": 2},
        ]
        mock_cursor = MagicMock()
        mock_cursor.__enter__.return_value = iter(docs)
        mock_collection.aggregate.return_value = mock_cursor

        df = TestModel.load_dataframe(filter=Filter({"value": {"$gt": 0}}))
        
        mock_collection.aggregate.assert_called_once()
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 2)
        self.assertIn("name", df.columns)

    @patch.object(TestModel, 'get_db_collection')
    def test_exists(self, mock_get_collection):
        """Test the exists method."""
        mock_collection = MagicMock()
        mock_get_collection.return_value = mock_collection

        # Test when document exists
        mock_collection.find_one.return_value = {"_id": ObjectId()}
        self.assertTrue(TestModel.exists(name="test"))
        mock_collection.find_one.assert_called_with({"name": "test"})

        # Test when document does not exist
        mock_collection.find_one.return_value = None
        self.assertFalse(TestModel.exists(name="nonexistent"))
        mock_collection.find_one.assert_called_with({"name": "nonexistent"})

    @patch.object(TestModel, 'get_db')
    def test_create_collection(self, mock_get_db):
        """Test collection creation logic."""
        mock_db = MagicMock()
        mock_get_db.return_value = mock_db
        collection_name = TestModel.get_db_collection_name()

        # Scenario 1: Collection does not exist
        mock_db.list_collection_names.return_value = ["another_collection"]
        TestModel.create_collection()
        mock_db.create_collection.assert_called_once_with(collection_name)

        # Scenario 2: Collection already exists
        mock_db.reset_mock()
        mock_db.list_collection_names.return_value = [collection_name]
        TestModel.create_collection()
        mock_db.create_collection.assert_not_called()


if __name__ == "__main__":
    unittest.main()
