# Loom

Weaves ideas and concept using information and time construct.  

This is my way to frame reality.  It is not the right way nor is it the only way, just simply a way that is helpful.

## Philosophy

-   **Lower mental load**: Abstract away boilerplate and repetitive tasks, letting developers focus on logic.
-   **Promote simple, elegant code**: Prioritize clarity and simplicity over exhaustive features. We are willing to sacrifice capability for a simpler, more focused codebase. For example, Loom is intentionally built only for MongoDB to avoid the complexity of supporting multiple database paradigms.  It is unlikely that we can add in other database and still be simple.
-   **Make the safe way the easy way**: The framework should guide developers away from common pitfalls, especially in data and time management.  Create coding friction for unsafe practices. But does not exhaustively build out guardrail if it comes at the cost of simplicity.

## Why?

Minimize path of thinking to reduce mental load.  There are a lot of ways to do thing in Python, this is a collection of way that aligned in our philosophy.

Promote and glue together mental model that work well together and align with my need
- [Apache Arrow](https://arrow.apache.org/)
- [Pydantic](https://pydantic-docs.helpmanual.io/)
- [MongoDB](https://www.mongodb.com/)
- [PyMongoArrow](https://www.mongodb.com/docs/languages/python/pymongo-arrow-driver/current/)
- [Arrow (Time)](https://arrow.readthedocs.io/en/latest/index.html)



## Information Management

The core of Loom's information management is a declarative persistence layer built on top of MongoDB, Pydantic, and PyMongoArrow

### PyMongoArrow: The Essential Bridge
PyMongoArrow is a PyMongo extension that serves as the essential bridge between MongoDB and the Arrow ecosystem. Its purpose is to load MongoDB query result-sets directly into high-performance analytical structures. It is, in fact, the "recommended way to materialize MongoDB query result-sets as contiguous-in-memory typed arrays suited for in-memory analytical processing applications." PyMongoArrow can materialize data into several key data structures favored by data scientists and analysts: • Apache Arrow tables • NumPy arrays • Pandas DataFrames • Polars DataFrames By providing a direct conversion path to these industry-standard formats, PyMongoArrow dramatically simplifies the data access layer for analytical applications built on top of MongoDB.

The data flow begins with a query to a MongoDB database. This blueprint bypasses the costly, row-by-row object process common in traditional database drivers. Instead, PyMongoArrow materializes the entire result set directly into the Arrow columnar format in memory. The critical outcome of this process is the elimination of the data serialization and deserialization steps that plague traditional data transfer architectures. By creating Arrow-native structures directly from the database results, applications can fully leverage the format's support for zero-copy reads. This directly enables the "lightning-fast data access" that is Arrow's core promise, removing a significant performance bottleneck and creating a highly efficient pipeline for in-memory computation.

### Quick Start

Getting started with Loom is simple. Define your model, decorate it, and start interacting with the database.

```python
from loom.info.persist import Persistable, declare_persist_db
import loom as lm

# 1. Define a Pydantic model and make it Persistable
# The `_id`, `created_at`, and `updated_time` fields are handled automatically.
@declare_persist_db(db_name="my_app", collection_name="users")
class User(Persistable):
    name: str
    email: str
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
active_users = User.filter(lm.fld('age') < 40).load_many()
print(f"Found {len(active_users)} active users.")

# 6. Create another user to persist multiple documents at once
another_user = User(name="Bob", email="bob@example.com", age=25)
User.persist_many([new_user, another_user])
```

### Core Concepts

-   **`Model` & `Persistable`**: Your data models inherit from `loom.info.model.Model` and mix in `loom.info.persist.Persistable`. This gives them database-aware capabilities.
-   **`@declare_persist_db`**: A class decorator that links your model to a specific MongoDB database and collection. It keeps your persistence logic clean and co-located with your data definition.
-   **Specialized Persistence Models**:
    -   **`LedgerModel`**: For append-only data. Every `persist()` call creates a new document, ensuring an immutable history.
    -   **`TimeSeriesLedgerModel`**: Extends `LedgerModel` for use with MongoDB's native time-series collections, configured with the `@declare_timeseries` decorator.
-   **Fluent Query API**: Loom provides a fluent and chainable API for building queries, starting with `YourModel.filter()` or `YourModel.aggregation()`. This returns a `LoadDirective` object that you can use to build and execute your query.
-   **Expressive Filtering with `fld`**: The `fld` object allows you to create filter expressions in a more Pythonic way (e.g., `fld('age') < 40`).
-   **Rich Querying and Loading**: The `LoadDirective` object provides a rich set of methods for loading data, including `load_one`, `load_many`, `load_latest`, `exists`, and methods for loading data into `pandas`, `polars`, and `pyarrow` data structures.
-   **Bulk Operations**: Use `persist_many` and `insert_dataframe` to efficiently save multiple model instances or a whole pandas DataFrame at once.

### The Query API

Loom 2.0 introduces a new fluent API for building and executing queries. This API is designed to be more expressive, more Pythonic, and easier to use than the previous query API.

#### Getting Started

To start building a query, use the `filter()` or `aggregation()` class methods on your `Persistable` model. These methods return a `LoadDirective` object, which you can use to build and execute your query.

```python
# Start a query with a filter
directive = User.filter(lm.fld('age') > 30)

# Start a query with an aggregation
directive = User.aggregation(Aggregation().Group(by="$name", sum_age=Aggregation.sum("$age")))
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
users = User.filter(lm.fld('age') > 30).sort(SortDesc('age')).limit(10).load_many()
```

#### Expressive Filtering with `fld`

The `fld` object provides a more Pythonic way to create filter expressions. You can use standard Python comparison operators to create filters.

```python
# Find users with age between 30 and 40
users = User.filter((lm.fld('age') >= 30) & (lm.fld('age') <= 40)).load_many()

# Find users with name 'Alice' or 'Bob'
users = User.filter(lm.fld('name').is_in(['Alice', 'Bob'])).load_many()
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
user = User.filter(lm.fld('name') == 'Alice').load_one()

# Load all users into a pandas DataFrame
df = User.filter().load_dataframe()
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
from loom.time.timeframing import DailyFrame, WeeklyFrame
from loom.time.util import EASTERN_TIMEZONE

# Get the current daily frame in US/Eastern time
today_eastern = DailyFrame(tzone=EASTERN_TIMEZONE)
print(f"Today (ET): {today_eastern.get_pretty_value()}")
print(f"Floor: {today_eastern.get_floor()}")
print(f"Ceiling: {today_eastern.get_ceiling()}")

# Get the previous frame
yesterday_eastern = today_eastern.get_previous_frame()
print(f"Yesterday (ET): {yesterday_eastern.get_pretty_value()}")

# Get the weekly frame for a specific moment
weekly_frame = WeeklyFrame(moment=yesterday_eastern.floor)
print(f"Weekly Frame: {weekly_frame.get_pretty_value()}")
```

## Fabric: A Time-Aware Toolkit

The Fabric layer provides higher-level components for building applications that reason about events over time.

-   **`Moment`**: A core `Persistable` model that represents a single, timestamped event or data point, identified by a `symbol`.
-   **`MomentWindow`**: A container for a sequence of `Moment` objects, sorted by time. It provides powerful methods for analysis, such as:
    -   `sliding_window(size)`: Yields consecutive windows of a fixed number of moments.
    -   `sliding_time_cone(past_size, future_size)`: Yields a tuple of (past_window, future_window), ideal for creating training data for predictive models.
-   **`insight` Toolkit**: A collection of Pydantic models for performing statistical analysis on time-series data.
    -   **`Spread`**: Calculates percentiles, standard deviation, and mean.
    -   **`Regression`**: Performs linear regression to find the slope and statistical significance of a trend.
    -   **`PdfValue`**: Uses Kernel Density Estimation (KDE) to find the most likely value (peak) and confidence interval of a dataset.
    -   **`Trending`**: A composite model that combines `Spread`, `Regression`, and `PdfValue` for a comprehensive trend analysis.
