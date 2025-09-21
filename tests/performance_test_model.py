from datetime import datetime
import loom as lm

@lm.declare_persist_db(collection_name="perf_test_data", db_name="test_db", test=True)
class PerformanceTestModel(lm.Persistable):
    name: str
    value: float
    value2: float
    date: datetime
    notes: str
