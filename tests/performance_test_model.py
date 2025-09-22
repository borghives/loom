from datetime import datetime

from loom.info import Persistable, declare_persist_db

@declare_persist_db(collection_name="perf_test_data", db_name="test_db", test=True)
class PerformanceTestModel(Persistable):
    name: str
    value: float
    value2: float
    date: datetime
    notes: str
