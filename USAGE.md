# Loom Usage Guide

This document provides a comprehensive guide to using the Loom library for building data-intensive applications with MongoDB.

## Table of Contents

1. [Defining Models](#defining-models)
2. [CRUD Operations](#crud-operations)
3. [Querying](#querying)
4. [Ledger Models](#ledger-models)
5. [Timeseries Models](#timeseries-models)
6. [Dataframe Integration](#dataframe-integration)

## Defining Models

Loom models are Pydantic models enhanced with persistence capabilities. To define a model, inherit from `Persistable` and decorate the class with `@declare_persist_db`.

### Basic Model

```python
import loom as lm
from loom.info import Persistable, declare_persist_db, StrLower

@declare_persist_db(db_name="my_app", collection_name="users")
class User(Persistable):
    name: str
    email: StrLower  # Automatically lowercased
    age: int
    is_active: bool = True
```

### Field Types

Loom provides specialized field types to handle common patterns:

- **`StrLower` / `StrUpper`**: Strings that are automatically normalized to lowercase or uppercase.
- **`TimeInserted`**: A timestamp set automatically when the document is first created.
- **`TimeUpdated`**: A timestamp updated automatically on every save.
- **`IncrCounter`**: An integer field that supports atomic increments (use `+=` on the object).

```python
from loom.info import TimeInserted, TimeUpdated, IncrCounter

@declare_persist_db(db_name="my_app", collection_name="stats")
class UserStats(Persistable):
    user_id: str
    login_count: IncrCounter = 0
    created_at: TimeInserted
    last_updated: TimeUpdated
```

## CRUD Operations

### Create

To create a new document, instantiate the model and call `persist()`.

```python
user = User(name="Alice", email="Alice@Example.com", age=30)
user.persist() # Saves to MongoDB, 'email' becomes 'alice@example.com'
print(f"User ID: {user.id}")
```

### Read

Load documents using `from_id` or the query API.

```python
# Load by ID
user = User.from_id("64f8a...")

# Load one matching a filter
user = User.filter(lm.fld['email'] == "alice@example.com").load_one()
```

### Update

Modify attributes and call `persist()` again. Loom tracks changes and performs efficient partial updates (`$set`).

```python
user.age = 31
user.persist() # Only updates the 'age' and 'updated_time' fields
```

### Atomic Increments

For `IncrCounter` fields, use the `+=` operator. Loom translates this into a MongoDB `$inc` operation.

```python
stats = UserStats.filter(lm.fld['user_id'] == user.id).load_one()
stats.login_count += 1
stats.persist() # Executes {$inc: {login_count: 1}}
```

## Querying

Loom offers a fluent API for building queries, similar to Polars or Django.

### Basic Filtering

Use `Model.filter()` to start a query. Access fields using `lm.fld['field_name']`.

```python
# Find active users older than 25
users = User.filter(
    (lm.fld['age'] > 25) & (lm.fld['is_active'] == True)
).load_many()
```

### Sorting and Limiting

Chain `sort()` and `limit()` methods.

```python
# Get top 10 oldest users
oldest_users = User.filter()\
    .sort('age', descending=True)\
    .limit(10)\
    .load_many()
```

### Aggregation

For complex analytics, use the aggregation pipeline.

```python
from loom.info import AggregationStages

pipeline = AggregationStages().group({
    "_id": "$is_active",
    "avg_age": {"$avg": "$age"}
})

results = User.agg(pipeline).load_many()
```

## Ledger Models

`LedgerModel` is designed for append-only data. Every `persist()` call creates a new document, preserving history.

```python
from loom.info import LedgerModel

@declare_persist_db(db_name="audit", collection_name="logs")
class AuditLog(LedgerModel):
    action: str
    user_id: str

log = AuditLog(action="login", user_id="123")
log.persist() # Creates document A

log.action = "logout"
log.persist() # Creates document B (new document)
```

### Bulk Persistence

Use `persist_many` for efficient bulk inserts.

```python
logs = [
    AuditLog(action="view", user_id="123"),
    AuditLog(action="click", user_id="123")
]
AuditLog.persist_many(logs)
```

## Timeseries Models

`TimeSeriesLedgerModel` leverages MongoDB's native time-series collections.

```python
from loom.info import TimeSeriesLedgerModel, declare_timeseries

@declare_timeseries(metakey="sensor_id", granularity="minutes", ttl=86400)
@declare_persist_db(db_name="iot", collection_name="readings")
class SensorReading(TimeSeriesLedgerModel):
    sensor_id: str
    temperature: float
    humidity: float

# Usage is the same as LedgerModel
reading = SensorReading(sensor_id="S1", temperature=22.5, humidity=45.0)
reading.persist()
```

## Dataframe Integration

Loom integrates seamlessly with Pandas, Polars, and PyArrow for high-performance data loading.

### Loading into Dataframes

```python
# Load as Pandas DataFrame
df = User.filter(lm.fld['age'] > 20).load_dataframe()

# Load as Polars DataFrame
pdf = User.filter().load_polars()

# Load as PyArrow Table
table = User.filter().load_table()
```

### Inserting Dataframes

Efficiently insert large datasets from Dataframes.

```python
import pandas as pd

df = pd.DataFrame({
    "name": ["Bob", "Charlie"],
    "email": ["bob@example.com", "charlie@example.com"],
    "age": [25, 35]
})

User.insert_dataframe(df)
```
