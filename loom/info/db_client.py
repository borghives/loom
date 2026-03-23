from loom.info.universal import LOCAL_MONGODB_CLIENT_URI
import asyncio
import threading
import os
from pymongo import AsyncMongoClient, MongoClient
from abc import ABC

class DbClientFactory(ABC):
    _t_client_cache_data = threading.local()

    def __init__(self, **kvargs):
        pass
    
    def get_client_uri(self) -> str:
        raise NotImplementedError
    
    def get_client_async(self) -> AsyncMongoClient:
        template_uri = self.get_client_uri()
        client_uri  = os.path.expandvars(template_uri)

        loop_id = id(asyncio.get_running_loop()) if asyncio.get_event_loop().is_running() else 0
        cache_key = f"{type(self).__name__}+{hash(template_uri)}+async+{loop_id}"

        db_client = getattr(self._t_client_cache_data, cache_key, None)
        if db_client is None:
            db_client = AsyncMongoClient(client_uri, tz_aware=True)
            setattr(self._t_client_cache_data, cache_key, db_client)
        
        assert (db_client is not None)
        return db_client
    
    def get_client(self) -> MongoClient:
        template_uri = self.get_client_uri()
        client_uri  = os.path.expandvars(template_uri)

        cache_key = f"{type(self).__name__}+{hash(template_uri)}+synced"
        db_client = getattr(self._t_client_cache_data, cache_key, None)
        if db_client is None:
            db_client = MongoClient(client_uri, tz_aware=True)
            setattr(self._t_client_cache_data, cache_key, db_client)
        
        assert (db_client is not None)
        return db_client
        
class UriClientFactory(DbClientFactory):
    def __init__(self, uri: str = "", **kvargs):
        super().__init__(**kvargs)

        self.uri = uri or os.getenv("MONGODB_URI") or LOCAL_MONGODB_CLIENT_URI

    def get_client_uri(self) -> str:
        return self.uri