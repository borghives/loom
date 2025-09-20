from loom.info.persist import Persistable, declare_persist_db

@declare_persist_db(collection_name="perf_test_data", db_name="test_db", test=True)
class PerformanceTestModel(Persistable):
    name: str
    value: float
    notes: str
