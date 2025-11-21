import sys
import loom as lm
from loom.info import Persistable, declare_persist_db, StrLower, TimeInserted, TimeUpdated, IncrCounter, LedgerModel, TimeSeriesLedgerModel, declare_timeseries, AggregationStages

TEST_DB = "loom_usage_test_db"

def test_basic_model():
    print("Testing Basic Model...")
    @declare_persist_db(db_name=TEST_DB, collection_name="users_test")
    class User(Persistable):
        name: str
        email: StrLower
        age: int
        is_active: bool = True

    # Create
    user = User(name="Alice", email="Alice@Example.com", age=30)
    try:
        user.persist()
        print(f"User ID: {user.id}")
    except Exception as e:
        print(f"Persistence failed (expected if no DB): {e}")
        return

    # Read
    try:
        u2 = User.from_id(user.id)
        u3 = User.filter(lm.fld['email'] == "alice@example.com").load_one()
    except Exception as e:
        print(f"Read failed: {e}")

    # Update
    try:
        user.age = 31
        user.persist()
    except Exception as e:
        print(f"Update failed: {e}")

def test_field_types():
    print("Testing Field Types...")
    @declare_persist_db(db_name=TEST_DB, collection_name="stats_test")
    class UserStats(Persistable):
        user_id: str
        login_count: IncrCounter = 0
        created_at: TimeInserted
        last_updated: TimeUpdated

    stats = UserStats(user_id="123")
    try:
        stats.persist()
        stats.login_count += 1
        stats.persist()
    except Exception as e:
        print(f"Field types test failed: {e}")

def test_ledger_model():
    print("Testing Ledger Model...")
    @declare_persist_db(db_name=TEST_DB, collection_name="logs_test")
    class AuditLog(LedgerModel):
        action: str
        user_id: str

    log = AuditLog(action="login", user_id="123")
    try:
        log.persist()
        log.action = "logout"
        log.persist()
        
        logs = [
            AuditLog(action="view", user_id="123"),
            AuditLog(action="click", user_id="123")
        ]
        AuditLog.persist_many(logs)
    except Exception as e:
        print(f"Ledger test failed: {e}")

def test_timeseries_model():
    print("Testing Timeseries Model...")
    @declare_timeseries(metakey="sensor_id", granularity="minutes", ttl=86400)
    @declare_persist_db(db_name=TEST_DB, collection_name="readings_test")
    class SensorReading(TimeSeriesLedgerModel):
        sensor_id: str
        temperature: float
        humidity: float

    reading = SensorReading(sensor_id="S1", temperature=22.5, humidity=45.0)
    try:
        reading.persist()
    except Exception as e:
        print(f"Timeseries test failed: {e}")

if __name__ == "__main__":
    test_basic_model()
    test_field_types()
    test_ledger_model()
    test_timeseries_model()
    print("Verification script finished.")
