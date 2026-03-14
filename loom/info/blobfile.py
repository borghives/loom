from pydantic import ConfigDict
from pydantic import BaseModel
import io
from abc import abstractmethod
from pydantic import Field

from loom.info import PersistableBase
from typing import List
from typing import Optional
from loom.info.db_fs_driver import MongoDbGridFSDriver
from loom.info.db_client import LocalClientFactory
from loom.info.db_client import DbClientFactory
from loom.info.index import Index

class BlobFileModel(PersistableBase):
    filename: str       = Field(default="")
    
    # model configuration
    model_config = ConfigDict(extra="ignore")

    @abstractmethod
    def dump_buffer(self) -> io.BytesIO:
        raise NotImplementedError

    def dump_metadata(self) -> Optional[dict]:
        if hasattr(self, "metadata"):
            if (self.metadata):
                if (isinstance(self.metadata, BaseModel)):
                    return self.metadata.model_dump(by_alias=True, exclude_none=True)
                elif (isinstance(self.metadata, dict)):
                    return self.metadata
        return None

    def persist_async(self, lazy: bool = False):
        raise NotImplementedError("persist async is not implemented for Blob File")

    def get_filename(self) -> str:
        return self.filename

    @classmethod
    def from_gridout(cls, gridout):
        return cls.from_doc({
            '_id'       : gridout._id, 
            'filename'  : getattr(gridout, 'filename', None), 
            'metadata'  : getattr(gridout, 'metadata', None),
        })

    def pull_file(self):
        fs=self.get_gridfs()

        if (self.is_entangled()):
            out = fs.get(self.collapse_id())
        else:
            out = fs.get_last_version(filename=self.filename)

        updated_model = self.from_gridout(out)
        self.__dict__.update(updated_model.__dict__)
        
        return out
            
    @classmethod
    def load_version(cls, filename: str, version: int):
        fs=cls.get_gridfs()
        return fs.get_version(filename=filename, version=version)

    @classmethod
    def get_gridfs(cls):
        driver = cls.get_model_driver()
        assert(isinstance(driver, MongoDbGridFSDriver))
        return driver.get_gridfs()
        
    def persist(self, lazy: bool = False):
        metadata_doc = self.dump_metadata()

        buffer = self.dump_buffer()
        if hasattr(buffer, 'seek'):
            buffer.seek(0)

        filename = self.get_filename()
        
        fs=self.get_gridfs()
        id = fs.put(buffer, filename=filename, metadata=metadata_doc)
        if (id):
            self.id = id

        return True

def declare_persist_fs(
    collection_name: str,
    db_name: str,
    client_factory: DbClientFactory = LocalClientFactory(),
    version: Optional[int] = None,
    index: Optional[List[Index]] = None,
    test: bool = False,
):
    """
    A class decorator to configure a `BlobFileModel` for a MongoDB
    GridFS collection.

    This decorator attaches the necessary metadata to the model class, which is
    then used by `BlobFileModel.create_collection` to set up the
    database collection correctly.

    """

    def decorator(cls):
        cls._db_model_driver = MongoDbGridFSDriver(
            collection_name=collection_name,
            db_name=db_name,
            client_factory=client_factory,
            version=version,
            index=index,
            test=test,
        )
        return cls

    return decorator

