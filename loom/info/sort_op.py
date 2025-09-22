from typing import List, Optional, Tuple

from pymongo import ASCENDING, DESCENDING

from loom.info.expression import Expression

class SortOp(Expression):
    def __init__(self, value: Optional[dict] = None) -> None:
        self._value = value

    @classmethod
    def wrap(cls, value) -> "SortOp":
        if isinstance(value, SortOp):
            return value

        if isinstance(value, dict):
            return cls(value)
        
        raise ValueError(f"unsupported type {type(value)} to wrap sort")

    def get_tuples(self) -> Optional[List[Tuple[str, int]]]:
        sort_values = self.express()
        if sort_values is None:
            return None

        return [(key, value) for key, value in sort_values.items()]

    def express(self):
        return self._value


class SortAsc(SortOp):
    def __init__(self, field: str) -> None:
        self._value = {field: ASCENDING}

class SortDesc(SortOp):
    def __init__(self, field: str) -> None:
        self._value = {field: DESCENDING}