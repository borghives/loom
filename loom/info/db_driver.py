
from typing import List
from typing import Optional
from loom.info.db_client import DbClientFactory
from loom.info.index import Index
from dataclasses import dataclass

from pymongo import MongoClient, AsyncMongoClient
from pymongo.database import Database
from pymongo.collection import Collection
from pymongo.asynchronous.database import AsyncDatabase
from pymongo.asynchronous.collection import AsyncCollection

@dataclass
class MongoDbModelDriver:
    collection_name: str
    db_name: str
    client_factory: DbClientFactory
    index: Optional[List[Index]] = None
    version: Optional[int] = None
    test: bool = False

    def get_db_name(self) -> str:
        """
        Gets the name of the MongoDB database for the model.

        Returns:
            str: The name of the MongoDB database.
        """
        return self.db_name

    def get_db_collection_name(self) -> str:
        """
        Gets the name of the MongoDB collection for the model.

        Returns:
            str: The name of the MongoDB collection.

        Raises:
            ValueError: If `collection_name` is not defined in the metadata.
        """
        if self.test:
            return f"{self.collection_name}_test"
        return self.collection_name

    def get_client_factory(self) -> DbClientFactory:
        """
        Gets the client factory for the model.

        Returns:
            DbClientFactory: The client factory for the model.
        """
        return self.client_factory
    
    def get_db_client(self, with_async: bool = False) -> MongoClient | AsyncMongoClient:
        """
        Gets the appropriate MongoDB client for the model (local or remote).

        Returns:
            MongoClient: The MongoDB client instance.
        """
        client_factory = self.get_client_factory()
        if (with_async):
            return client_factory.get_client_async()
        else:
            return client_factory.get_client()
        
    def get_db(self, withAsync: bool = False) -> Database | AsyncDatabase:
        """
        Gets the MongoDB database object for the model.

        Returns:
            Database: The `pymongo.database.Database` instance.
        """
        client = self.get_db_client(withAsync)
        db_name = self.get_db_name()
        return client[db_name]

    def get_db_collection(self, with_async: bool = False) -> Collection | AsyncCollection:
        """
        Gets the MongoDB collection object for the model.

        Returns:
            Collection: The `pymongo.collection.Collection` instance.
        """
        client_database = self.get_db(with_async)
        collection_name = self.get_db_collection_name()
        return client_database[collection_name]
    

    def create_collection(self, **kvargs):
        db = self.get_db()
        assert isinstance(db, Database)
        collection_names = db.list_collection_names()
        name = self.get_db_collection_name()

        if name not in collection_names:
            db.create_collection(name, **kvargs)

    async def create_collection_async(self, **kvargs):
        db = self.get_db(withAsync=True)
        assert isinstance(db, AsyncDatabase)
        collection_names = await db.list_collection_names()
        name = self.get_db_collection_name()

        if name not in collection_names:
            await db.create_collection(name, **kvargs)

    def create_index(self):
        indexes = self.index
        if indexes and len(indexes) > 0:
            collection = self.get_db_collection(with_async=False)
            assert isinstance(collection, Collection)
            for index in indexes:
                assert isinstance(index, Index)
                collection.create_index(**index.to_dict())

    async def create_index_async(self):
        indexes = self.index
        if indexes and len(indexes) > 0:
            collection = self.get_db_collection(with_async=True)
            assert isinstance(collection, AsyncCollection)
            for index in indexes:
                assert isinstance(index, Index)
                await collection.create_index(**index.to_dict())
    
