
from loom.info.expression import ExpressionDriver
from pymongo.asynchronous.collection import AsyncCollection
from typing import TypeVar, Protocol, Self
from pymongo.collection import Collection

class PersistableInterface(Protocol):
    @classmethod
    def get_init_collection(cls) -> Collection:
        ...

    @classmethod
    async def get_init_collection_async(cls) -> AsyncCollection:
        ...
    
    @classmethod
    def from_doc(cls, doc: dict) -> Self:
        ...

    @classmethod
    def get_mql_driver(cls) -> ExpressionDriver:
        ...

PersistableType = TypeVar("PersistableType", bound="PersistableInterface")