# Loom

Loom provides a structured way to weave together ideas and concepts using information and time constructs.

It offers a pragmatic approach for framing complex reality model.

## Philosophy

-   **Lower mental load**: Abstract away boilerplate and repetitive tasks, letting developers focus on logic.
-   **Promote simple, elegant code**: Prioritize clarity and simplicity over exhaustive features. We are willing to sacrifice capability for a simpler, more focused codebase. For example, Loom is intentionally built only for MongoDB to avoid the complexity of supporting multiple database paradigms. Supporting other databases would likely compromise this simplicity.
-   **Make the safe way the easy way**: The framework guides developers away from common pitfalls, especially in data and time management. Though, it creates coding friction for unsafe practices, it does not build exhaustive guardrails at the cost of simplicity.

## Documentation

For detailed usage examples and comprehensive guides, see **[USAGE.md](USAGE.md)**.

## Why?

Loom aims to minimize the cognitive load on developers. While Python offers endless ways to accomplish tasks, Loom provides a collection of harmonized approaches that follow our philosophy.

It promotes and glues together mental models that work well together and align with our needs.
- [Apache Arrow](https://arrow.apache.org/)
- [Pydantic](https://pydantic-docs.helpmanual.io/)
- [MongoDB](https://www.mongodb.com/)
- [PyMongoArrow](https://www.mongodb.com/docs/languages/python/pymongo-arrow-driver/current/)
- [Arrow (Time)](https://arrow.readthedocs.io/en/latest/index.html)



## Information Management

The core of Loom's information management is a declarative persistence layer built on top of MongoDB, Pydantic, and PyMongoArrow

### PyMongoArrow: The Essential Bridge
PyMongoArrow is a PyMongo extension that serves as the essential bridge between MongoDB and the Arrow ecosystem. Its purpose is to load MongoDB query result-sets directly into high-performance analytical structures. It is, in fact, the "recommended way to materialize MongoDB query result-sets as contiguous-in-memory typed arrays suited for in-memory analytical processing applications." PyMongoArrow can materialize data into several key data structures favored by data scientists and analysts:

-   Apache Arrow tables
-   NumPy arrays
-   Pandas DataFrames
-   Polars DataFrames

By providing a direct conversion path to these industry-standard formats, PyMongoArrow dramatically simplifies the data access layer for analytical applications built on top of MongoDB.

The data flow begins with a query to a MongoDB database. This blueprint bypasses the costly, row-by-row object process common in traditional database drivers. Instead, PyMongoArrow materializes the entire result set directly into the Arrow columnar format in memory. The critical outcome of this process is the elimination of the data serialization and deserialization steps that plague traditional data transfer architectures. By creating Arrow-native structures directly from the database results, applications can fully leverage the format's support for zero-copy reads. This directly enables the "lightning-fast data access" that is Arrow's core promise, removing a significant performance bottleneck and creating a highly efficient pipeline for in-memory computation.

### Quick Start

Getting started with Loom is simple. Define your model, decorate it, and start interacting with the database.

```python
import loom as lm
from loom.info import Persistable, declare_persist_db, StrLower

# 1. Define a Pydantic model and make it Persistable
# The `_id`, `created_at`, and `updated_time` fields are handled automatically.
@declare_persist_db(db_name="my_app", collection_name="users")
class User(Persistable):
    name: str
    email: StrLower #StrLower is an annotated str type that will normalized to lower case (querying and filtering will also lowercase the search input)
    age: int

# 2. Create and save a new user
new_user = User(name="Alice", email="alice@example.com", age=30)
new_user.persist()
print(f"Saved user with ID: {new_user.id}")

# 3. Load a user from the database using its ID
retrieved_user = User.from_id(new_user.id)
if retrieved_user:
    print(f"Found user: {retrieved_user.name}")

# 4. Update the user's age
retrieved_user.age = 31
retrieved_user.persist()
print(f"User's updated time: {retrieved_user.updated_time}")

# 5. Load multiple users with a filter
active_users = User.filter(lm.fld['age'] < 40).load_many()
print(f"Found {len(active_users)} active users.")

# 6. Create other users to persist multiple documents at once
user_bob = User(name="Bob", email="bob@example.com", age=25)
user_charlie = User(name="Charlie", email="charlie@example.com", age=35)
User.persist_many([user_bob, user_charlie])

# 7. Load one user with a filter
bob_user = User.filter(lm.fld['email'] == "BOB@example.COM").load_one()
if bob_user:
    print(f"Found Bob: {bob_user.name} with email: {bob_user.email}")

```

### Core Concepts

-   **`Persistable`**: Your data models inherit from `lm.Persistable` to gain database-aware capabilities.
-   **`@declare_persist_db`**: A class decorator that links your model to a specific MongoDB database and collection. It keeps your persistence logic clean and co-located with your data definition.
-   **Specialized Persistence Models**:
    -   **`LedgerModel`**: For append-only data. Every `persist()` call creates a new document, ensuring an immutable history.
    -   **`TimeSeriesLedgerModel`**: Extends `LedgerModel` for use with MongoDB's native time-series collections, configured with the `@declare_timeseries` decorator.
-   **Fluent Query API**: Loom provides a fluent and chainable API for building queries, starting with `YourModel.filter()` or `YourModel.aggregation()`. This returns a `LoadDirective` object that you can use to build and execute your query.
-   **Expressive Filtering with `Model.fields()`**: The `YourModel.fields()` class method returns a dictionary-like object that allows you to create filter expressions in a more Pythonic way (e.g., `User.fields()['age'] < 40`).
-   **Rich Querying and Loading**: The `LoadDirective` object provides a rich set of methods for loading data, including `load_one`, `load_many`, `load_latest`, `exists`, and methods for loading data into `pandas`, `polars`, and `pyarrow` data structures.
-   **Bulk Operations**: Use `persist_many` and `insert_dataframe` to efficiently save multiple model instances or a whole DataFrame at once.

### The Query API

Loom 2.0 introduces a new fluent API for building and executing queries. This API is designed to be more expressive, more Pythonic, and easier to use than the previous query API. It borrows familiarity from the Polars API to provide a consistent learning curve.  

#### Getting Started

To start building a query, use the `filter()` or `aggregation()` class methods on your `Persistable` model. These methods return a `LoadDirective` object, which you can use to build and execute your query.

```python
# Start a query with a filter
directive = User.filter(lm.fld['age'] > 30)

# Start a query with an aggregation
directive = User.aggregation(lm.Aggregation().group({"_id": "$name", "sum_age": {"$sum": "$age"}}))
```

#### Building Queries

The `LoadDirective` object provides a number of methods for building queries, including:

- `filter(filter)`: Add a filter to the query.
- `sort(sort)`: Add a sort to the query.
- `limit(limit)`: Add a limit to the query.
- `aggregation(aggregation)`: Add an aggregation pipeline to the query.

These methods are chainable, so you can build complex queries in a single expression.

```python
# Build a query with a filter, sort, and limit
users = User.filter(lm.fld['age'] > 30).sort('age', descending=True).limit(10).load_many()
```

#### Expressive Filtering with `Model.fields()`

The `Model.fields()` method provides a more Pythonic way to create filter expressions. You can use standard Python comparison operators to create filters.

```python
# Find users with age between 30 and 40
users = User.filter((lm.fld['age'] >= 30) & (lm.fld['age'] <= 40)).load_many()

# Find users with name 'Alice' or 'Bob'
users = User.filter(lm.fld['name'].is_in(['Alice', 'Bob'])).load_many()
```

#### Loading Data

Once you have built your query, you can use one of the `load_*` methods to execute it and get the results.

- `load_one()`: Load a single document.
- `load_many()`: Load multiple documents.
- `load_latest()`: Load the most recently updated document.
- `exists()`: Check if a document exists.
- `load_dataframe()`: Load the results into a pandas DataFrame.
- `load_polars()`: Load the results into a polars DataFrame.
- `load_table()`: Load the results into a pyarrow Table.

```python
# Load a single user
user = User.filter(lm.fld['name'] == 'Alice').load_one()

# Load all users into a pandas DataFrame
df = User.filter().load_dataframe()

# Load all users into a polars DataFrame
polars_df = User.filter().load_polars()

# Load all users into a pyarrow Table
table = User.filter().load_table()
```

### Automatic Field Behaviors

Loom uses Python's `Annotated` type to attach special behaviors to your model fields, reducing boilerplate and ensuring consistency.

-   **`TimeInserted` & `TimeUpdated`**: Automatically manage `created_at` and `updated_time` timestamps. `TimeInserted` is set once on creation, and `TimeUpdated` is refreshed on every `persist()` call.
-   **`StrUpper` & `StrLower`**: Automatically normalize string fields to uppercase or lowercase.
-   **`IncrCounter`**: An integer type for atomic increments. Use the `+=` operator on the field, and Loom will translate it into an `$inc` operation in the database.

### Initial limitations for simplicity:
- Support for MongoDB only.
- Secret management is handled via `google-cloud-secret-manager`, local `keyring`, or environment variable.

## Time Management

Loom provides a structured way to reason about time through `TimeFrame` objects, which represent discrete, human-understandable intervals.

### Core Concepts

-   **`TimeFrame`**: The base class for all time frames. It defines a `floor` (inclusive) and a `ceiling` (exclusive).
-   **Resolutions**: Loom provides concrete `TimeFrame` implementations for common resolutions: `HourlyFrame`, `DailyFrame`, `WeeklyFrame`, `MonthlyFrame`, `QuarterlyFrame`, and `YearlyFrame`.
-   **Timezone Aware**: Time frames are timezone-aware, defaulting to UTC but easily configurable for any timezone.

### Quick Start

```python
import loom as lm

# Get the current daily frame in US/Eastern time
today_eastern = lm.DailyFrame.create(tzone=lm.EASTERN_TIMEZONE)
print(f"Today (ET): {today_eastern.get_pretty_value()}")
print(f"Floor: {today_eastern.get_floor()}")
print(f"Ceiling: {today_eastern.get_ceiling()}")

# Get the previous frame
yesterday_eastern = today_eastern.get_previous_frame()
print(f"Yesterday (ET): {yesterday_eastern.get_pretty_value()}")

# Get the weekly frame for a specific moment
weekly_frame = lm.WeeklyFrame.create(moment=yesterday_eastern.floor)
print(f"Weekly Frame: {weekly_frame.get_pretty_value()}")
```

## A Time-Aware Toolkit

-   **`Moment`**: A core `Persistable` model that represents a single, timestamped event or data point, identified by a `symbol`.
-   **`MomentWindow`**: A container for a sequence of `Moment` objects, sorted by time. It provides powerful methods for analysis, such as:
    -   `sliding_window(size)`: Yields consecutive windows of a fixed number of moments.
    -   `sliding_time_cone(past_size, future_size)`: Yields a tuple of (past_window, future_window), ideal for creating training data for predictive models.
-   **`insight` Toolkit**: A collection of Pydantic models for performing statistical analysis on time-series data.
    -   **`Spread`**: Calculates percentiles, standard deviation, and mean.
    -   **`Regression`**: Performs linear regression to find the slope and statistical significance of a trend.
    -   **`PdfValue`**: Uses Kernel Density Estimation (KDE) to find the most likely value (peak) and confidence interval of a dataset.
    -   **`Trending`**: A composite model that combines `Spread`, `Regression`, and `PdfValue` for a comprehensive trend analysis.