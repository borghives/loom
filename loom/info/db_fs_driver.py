
from pymongo.synchronous.database import Database
from loom.info.db_driver import MongoDbModelDriver
import gridfs

class MongoDbGridFSDriver(MongoDbModelDriver):
    def get_db_collection_name(self) -> str:
        """
        Gets the name of the MongoDB collection for the model.

        Returns:
            str: The name of the MongoDB collection.

        Raises:
            ValueError: If `collection_name` is not defined in the metadata.
        """
        collection_name = super().get_db_collection_name()
        return f"{collection_name}.files"

    def get_gridfs(self):
        db = self.get_db()
        collection_name = super().get_db_collection_name()
        assert isinstance(db, Database)
        return gridfs.GridFS(db, collection=collection_name)

    def create_collection(self, **kvargs):
        #do nothing
        pass

    async def create_collection_async(self, **kvargs):
        #do nothing
        pass