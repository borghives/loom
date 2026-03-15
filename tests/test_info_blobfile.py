from typing import Optional
import unittest
import io
from bson import ObjectId
from pydantic import Field, BaseModel

from loom.info.blobfile import BlobFileModel, declare_persist_fs
from pymongo.collection import Collection
from pymongo.database import Database

class SampleMetadata(BaseModel):
    author: str
    topic: str

@declare_persist_fs(db_name="test_db", collection_name="test_blob_collection", test=True)
class SampleBlobModel(BlobFileModel):
    filename: str
    metadata : Optional[SampleMetadata] = None
    content: bytes = b""


    def dump_buffer(self) -> io.BytesIO:
        return io.BytesIO(self.content)




class TestBlobFile(unittest.TestCase):
    def setUp(self):
        collection = SampleBlobModel.get_init_collection()
        assert isinstance(collection, Collection)
        collection.delete_many({})
        
        db = SampleBlobModel.get_model_driver().get_db(with_async=False)
        assert isinstance(db, Database)
        db["test_blob_collection.chunks"].delete_many({})
        
    def test_persist_blob(self):
        blob = SampleBlobModel(filename="test.txt", content=b"hello world")
        result = blob.persist()
        self.assertTrue(result)
        self.assertIsNotNone(blob.id)
        
        # Read it back using gridfs
        fs = blob.get_gridfs()
        out = fs.open_download_stream(blob.id)
        self.assertEqual(out.read(), b"hello world")
        self.assertEqual(out.filename, "test.txt")

    def test_persist_blob_with_metadata(self):
        meta = SampleMetadata(author="bot", topic="testing")
        blob = SampleBlobModel(filename="meta_test.txt", content=b"metadata content", metadata=meta)
        result = blob.persist()
        self.assertTrue(result)
        self.assertIsNotNone(blob.id)

        in_content = blob.open_read_file()
        self.assertEqual(in_content.read(), b"metadata content")

        fs = SampleBlobModel.get_gridfs()
        in_content2 = fs.open_download_stream(blob.id)
        self.assertEqual(in_content2.read(), b"metadata content")
        self.assertEqual(in_content2.filename, "meta_test.txt")
        self.assertIsNotNone(in_content2.metadata)
        self.assertEqual(in_content2.metadata["author"], "bot")
        self.assertEqual(in_content2.metadata["topic"], "testing")

        recall = SampleBlobModel.from_id(blob.id)
        self.assertIsNotNone(recall.metadata)
        self.assertEqual(recall.metadata.author, "bot")
        self.assertEqual(recall.metadata.topic, "testing")

        # Test that get_file updates an empty model instance
        empty_blob = SampleBlobModel(filename="meta_test.txt")
        in_content_empty = empty_blob.open_read_file()
        self.assertEqual(in_content_empty.read(), b"metadata content")
        self.assertEqual(empty_blob.id, blob.id)
        self.assertIsNotNone(empty_blob.metadata)
        self.assertEqual(empty_blob.metadata.author, "bot")
        self.assertEqual(empty_blob.metadata.topic, "testing")

        blob2 = SampleBlobModel(filename="meta_test.txt", content=b"metadata content 2", metadata=meta)
        result = blob2.persist()


        out_v1 = SampleBlobModel.load_version("meta_test.txt", 0)
        self.assertEqual(out_v1.read(), b"metadata content")

        out_v2 = SampleBlobModel.load_version("meta_test.txt", 1)
        self.assertEqual(out_v2.read(), b"metadata content 2")

    def test_load_version(self):
        blob1 = SampleBlobModel(filename="versioned.txt", content=b"v1")
        blob1.persist()
        
        blob2 = SampleBlobModel(filename="versioned.txt", content=b"v2")
        blob2.persist()
        
        out_v1 = SampleBlobModel.load_version("versioned.txt", 0)
        self.assertEqual(out_v1.read(), b"v1")
        
        out_v2 = SampleBlobModel.load_version("versioned.txt", 1)
        self.assertEqual(out_v2.read(), b"v2")
        
        out_latest = SampleBlobModel.load_version("versioned.txt", -1)
        self.assertEqual(out_latest.read(), b"v2")


class TestAsyncBlobFile(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        collection = await SampleBlobModel.get_init_collection_async()
        from pymongo.asynchronous.collection import AsyncCollection
        assert isinstance(collection, AsyncCollection)
        await collection.delete_many({})
        
        db = SampleBlobModel.get_model_driver().get_db(with_async=True)
        from pymongo.asynchronous.database import AsyncDatabase
        assert isinstance(db, AsyncDatabase)
        await db["test_blob_collection.chunks"].delete_many({})

    async def test_persist_async(self):
        blob = SampleBlobModel(filename="async_test.txt", content=b"hello async world")
        result = await blob.persist_async()
        self.assertTrue(result)
        self.assertIsNotNone(blob.id)

        # Read it back using gridfs
        fs = SampleBlobModel.get_gridfs_async()
        out = await fs.open_download_stream(blob.id)
        content = await out.read()
        self.assertEqual(content, b"hello async world")
        self.assertEqual(out.filename, "async_test.txt")

    async def test_open_read_file_async(self):
        meta = SampleMetadata(author="async_bot", topic="async_testing")
        blob = SampleBlobModel(filename="async_meta_test.txt", content=b"async metadata content", metadata=meta)
        result = await blob.persist_async()
        self.assertTrue(result)
        self.assertIsNotNone(blob.id)

        empty_blob = SampleBlobModel(filename="async_meta_test.txt")
        out_empty = await empty_blob.open_read_file_async()
        content = await out_empty.read()
        
        self.assertEqual(content, b"async metadata content")
        self.assertEqual(empty_blob.id, blob.id)
        self.assertIsNotNone(empty_blob.metadata)
        self.assertEqual(empty_blob.metadata.author, "async_bot")
        self.assertEqual(empty_blob.metadata.topic, "async_testing")

if __name__ == "__main__":
    unittest.main()
