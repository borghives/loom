from typing import Optional
from dataclasses import dataclass
from pymongo.operations import UpdateOne, InsertOne
from abc import ABC, abstractmethod

class PersistOperation(ABC):
    @abstractmethod
    def to_operation(self) -> UpdateOne | InsertOne:
        pass

@dataclass
class UpdateOneOperation(PersistOperation):
    filter : dict
    update : dict
    upsert : bool = False

    def to_operation(self) -> UpdateOne:
        return UpdateOne(filter=self.filter, update=self.update, upsert=self.upsert)

@dataclass
class InsertOneOperation(PersistOperation):
    document : dict

    def to_operation(self) -> InsertOne:
        return InsertOne(document=self.document)