from loom.info.universal import access_secret
import threading
import os
from pymongo import AsyncMongoClient, MongoClient
from abc import ABC

LOCAL_MONGODB_CLIENT_URI = "mongodb://localhost:%s/"

class DbClientFactory(ABC):
    _t_client_cache_data = threading.local()

    def __init__(self, **kvargs):
        pass
    
    def get_client_uri(self) -> str:
        raise NotImplementedError
    
    def get_client_async(self) -> AsyncMongoClient:
        client_uri = self.get_client_uri()
        cache_key = f"{type(self).__name__}+{hash(client_uri)}+async"
        db_client = getattr(self._t_client_cache_data, cache_key, None)
        if db_client is None:
            db_client = AsyncMongoClient(self.get_client_uri(), tz_aware=True)
            setattr(self._t_client_cache_data, cache_key, db_client)
        
        assert (db_client is not None)
        return db_client
    
    def get_client(self) -> MongoClient:
        client_uri = self.get_client_uri()
        cache_key = f"{type(self).__name__}+{hash(client_uri)}+synced"
        db_client = getattr(self._t_client_cache_data, cache_key, None)
        if db_client is None:
            db_client = MongoClient(self.get_client_uri(), tz_aware=True)
            setattr(self._t_client_cache_data, cache_key, db_client)
        
        assert (db_client is not None)
        return db_client
        
class LocalClientFactory(DbClientFactory):
    def __init__(self, port: int = 27017, **kvargs):
        super().__init__(**kvargs)
        self.port = port

    def get_client_uri(self) -> str:
        return (os.getenv("MONGODB_LOCAL_URI") or LOCAL_MONGODB_CLIENT_URI) % str(self.port)

class RemoteClientFactory(DbClientFactory):
    def __init__(self, uri: str = "", secret_name: str = "", **kvargs):
        super().__init__(**kvargs)

        mongodb_uri = uri or os.getenv("MONGODB_URI")

        if mongodb_uri is None:
            raise Exception("MONGODB_URI not set for remote client")

        self.uri = mongodb_uri.strip()

        if len(self.uri) == 0:
            raise Exception("Mongo Db URI is empty")

        secret_id = secret_name or os.getenv("MONGODB_SECRET")
        if (secret_id is None) or (len(secret_id) == 0):
            raise Exception(
                '''
                MONGODB_SECRET or a secret id is not set for remote client.  Remote Client MUST 
                have a secret id to fetch the secret part of the remote database uri.  For safety reason there should 
                always be a secret part to the remote database uri.  HINT: Extend the get_access_secret 
                function to support a secret manager of your choice.
                '''
            )

        self.secret_name = secret_id

    def get_access_secret(self) -> str:
        return access_secret(self.secret_name)
    
    def get_client_uri(self) -> str:
        return self.uri % self.get_access_secret()