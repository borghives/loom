## PyMongoArrow: The Essential Bridge
PyMongoArrow is a PyMongo extension that serves as the essential bridge between MongoDB and the Arrow ecosystem. Its purpose is to load MongoDB query result-sets directly into high-performance analytical structures. It is, in fact, the "recommended way to materialize MongoDB query result-sets as contiguous-in-memory typed arrays suited for in-memory analytical processing applications."
PyMongoArrow can materialize data into several key data structures favored by data scientists and analysts:
• Apache Arrow tables
• NumPy arrays
• Pandas DataFrames
• Polars DataFrames
By providing a direct conversion path to these industry-standard formats, PyMongoArrow dramatically simplifies the data access layer for analytical applications built on top of MongoDB.

## The Architectural Blueprint for High-Performance Data Transfer
The architectural blueprint enabled by this integration is both elegant and efficient. The data flow begins with a query to a MongoDB database. This blueprint bypasses the costly, row-by-row object hydration process common in traditional database drivers. Instead, PyMongoArrow materializes the entire result set directly into the Arrow columnar format in memory.
The critical outcome of this process is the elimination of the data serialization and deserialization steps that plague traditional data transfer architectures. By creating Arrow-native structures directly from the database results, applications can fully leverage the format's support for zero-copy reads. This directly enables the "lightning-fast data access" that is Arrow's core promise, removing a significant performance bottleneck and creating a highly efficient pipeline for in-memory computation.

## Technical Plan: Integrating PyMongoArrow into `Persistable` (Revised)

This plan outlines a revised, style-aligned strategy to integrate `pymongoarrow` into the `loom.info.persist.Persistable` class.

### 1. Objective

The primary objective remains to replace the current `pymongo` cursor-to-DataFrame conversion in `Persistable.load_dataframe` with a more performant method using `pymongoarrow`, leveraging zero-copy reads for faster data materialization.

### 2. Dependency Management

- **Add `pymongoarrow`**: This remains unchanged. The library will be added using Poetry:
  ```bash
  poetry add pymongoarrow
  ```

### 3. Core Integration Strategy (Refined)

The integration will follow the existing layered architecture of `Persistable`. We will introduce a new low-level aggregation method for Arrow and build upon it, rather than creating a parallel execution path. This maintains a clear separation of concerns.

1.  **`aggregate_arrow`**: A new private or protected class method that serves as the primary bridge to `pymongoarrow`.
2.  **`get_arrow_schema`**: A class method to dynamically generate the Arrow schema from the Pydantic model.
3.  **`load_arrow_table`**: A new public method for users who want to work directly with Arrow Tables.
4.  **`load_dataframe`**: The existing method will be updated to use `load_arrow_table` as its backend.

### 4. Schema Inference Strategy

The implementation deviates from the original plan of dynamic schema generation. Instead of manually mapping Pydantic types to `pyarrow` types, the `get_arrow_schema` method returns `None`, delegating the schema inference to `pymongoarrow`.

-   **`get_arrow_schema`**: This class method is simplified to return `None`. This allows `pymongoarrow` to infer the schema directly from the data returned by the MongoDB query.
-   **Flexibility**: This approach is more flexible as it doesn't require manual updates when the Pydantic model changes. However, it relies on `pymongoarrow`'s inference capabilities.
-   **Future Implementation**: The method is designed so that individual `Persistable` subclasses can override it to provide an explicit schema if needed.

### 5. New & Updated Methods

The implementation introduces a layered approach for data loading, with `load_dataframe` building on `load_arrow_table`, which in turn uses `aggregate_arrow`.

-   **`@classmethod def aggregate_arrow(cls, aggregation: Aggregation, schema: Optional[Schema]) -> pa.Table:`**
    - This is the core primitive for executing an aggregation pipeline and returning a `pyarrow.Table`.
    - **Implementation:**
        1.  Gets the `Collection` via `cls.get_db_collection()`.
        2.  Parses the `Aggregation` object into a pipeline via `cls.parse_agg_pipe(aggregation)`.
        3.  Executes the query using `pymongoarrow.aggregate.aggregate_arrow_all(collection, pipeline, schema=schema)`. The `schema` is optional.
        4.  Returns the resulting `pyarrow.Table`.

-   **`@classmethod def load_arrow_table(...) -> pyarrow.Table:`**
    - This is the public-facing method for loading data into an Arrow Table.
    - **Implementation:**
        1.  Constructs the `Aggregation` object from `filter`, `sort`, and `sampling`.
        2.  Calls `cls.get_arrow_schema()` (which returns `None`) to let `pymongoarrow` infer the schema.
        3.  Calls `cls.aggregate_arrow(aggregation, schema)` and returns the result.
    - **Note:** Unlike the original plan, this implementation does not prepend a stage to convert `_id` to a string. It relies on the default handling of `ObjectId` by `pymongoarrow` and `pyarrow`.

-   **`@classmethod def load_dataframe(...) -> pd.DataFrame:` (Updated)**
    - This method's signature remains unchanged, providing a transparent performance upgrade to existing code.
    - **Implementation:**
        1.  Calls `cls.load_arrow_table(...)` with all arguments.
        2.  Converts the resulting Arrow Table to a pandas DataFrame via `.to_pandas()`.
        3.  If an `_id` column exists, it is set as the DataFrame's index.
        4.  Returns the DataFrame.

### 6. Implemented Code Sketch

This sketch reflects the final implementation in `loom/info/persist.py`.

```python
# In loom/info/persist.py

class Persistable(Model):
    # ... existing code ...

    @classmethod
    def _load_dataframe_legacy(
        cls,
        aggregation: Optional[Aggregation] = None,
        filter: Filter = Filter(),
        sampling: Optional[Size] = None,
        sort: SortOp = SortOp()
    ) -> pd.DataFrame:
        """
        Loads data from an aggregation query into a pandas DataFrame.
        """
        with cls.aggregate(
            aggregation=aggregation,
            filter=filter,
            sampling=sampling,
            sort=sort,
        ) as cursor:
            df = pd.DataFrame(cursor)
            if "_id" in df.columns:
                df.set_index("_id", inplace=True)
            return df


    @classmethod
    def aggregate_arrow(cls, aggregation: Aggregation, schema: Optional[Schema]) -> pa.Table:
        """
        Executes an aggregation pipeline and returns a pyarrow.Table.
        """
        collection = cls.get_db_collection()
        pipeline = cls.parse_agg_pipe(aggregation)
        return aggregate_arrow_all(collection, pipeline, schema=schema)

    @classmethod
    def load_arrow_table(
        cls,
        aggregation: Optional[Aggregation] = None,
        filter: Filter = Filter(),
        sampling: Optional[Size] = None,
        sort: SortOp = SortOp()
    ) -> pa.Table:
        """
        Loads data from a query into a PyArrow Table.
        """
        if aggregation is None:
            aggregation = Aggregation()

        if filter.has_filter():
            aggregation = aggregation.Match(filter)
        if sampling:
            aggregation = aggregation.Sample(sampling)
        aggregation = aggregation.Sort(sort)
        
        schema = cls.get_arrow_schema()
        return cls.aggregate_arrow(aggregation, schema)
    
    @classmethod
    def load_dataframe(
        cls,
        aggregation: Optional[Aggregation] = None,
        filter: Filter = Filter(),
        sampling: Optional[Size] = None,
        sort: SortOp = SortOp()
    ) -> pd.DataFrame:
        """
        Loads data from an aggregation query into a pandas DataFrame using PyMongoArrow.
        """
        arrow_table = cls.load_arrow_table(
            aggregation=aggregation, filter=filter, sampling=sampling, sort=sort,
        )
        df = arrow_table.to_pandas()
        if "_id" in df.columns:
            df.set_index("_id", inplace=True)
        return df
    # --- END: PyArrow ---
    
    # ... rest of the class ...
```

### 7. Implementation Notes and Deviations

-   **Schema Generation**: The most significant deviation from the original plan is the schema generation strategy. The final implementation opts to have `pymongoarrow` infer the schema by default (by having `get_arrow_schema` return `None`). This simplifies the code and makes it more maintainable, as it doesn't require manual mapping between Pydantic and Arrow types. However, it relies on the quality of `pymongoarrow`'s type inference. The design allows for subclasses to provide an explicit schema by overriding `get_arrow_schema` if needed.

-   **`_id` Handling**: The plan to explicitly cast `_id` to a string within an aggregation pipeline was not implemented. The current code relies on `pyarrow`'s default handling of BSON `ObjectId` types. The final `load_dataframe` method correctly sets the `_id` column as the index in the resulting pandas DataFrame.


This revised plan is more respectful of the existing architecture, promotes code reuse, and is more explicit about its operations, making it a better fit for the project's established style.

### 8. Performance Testing Strategy

To validate the benefits of this integration, a robust performance test is required. This involves two key components: generating a sufficiently large dataset and creating a benchmark test to compare the "before" and "after" scenarios (A/B testing).

#### Data Generation

We will create a script to populate a test MongoDB collection with a large volume of realistic data.

-   **Tooling**: A standalone Python script using the `Faker` library to generate varied data types (names, dates, text, numbers).
-   **Schema**: The script will use a dedicated Pydantic model that inherits from `Persistable` to define the data structure. This model will be decorated with `declare_persist_db(..., test=True)` to ensure it uses a separate test collection.
-   **Insertion**: The script will generate a large number of documents (e.g., 100,000 or 1,000,000) and use the `insert_many` method for efficient bulk insertion.

**Example Data Generation Script Sketch (`scripts/generate_test_data.py`):**

```python
import random
from faker import Faker
from loom.info.persist import Persistable, declare_persist_db

# 1. Define a test model
@declare_persist_db(collection_name="perf_test_data", db_name="test_db", test=True)
class PerformanceTestModel(Persistable):
    name: str
    value: float
    notes: str

# 2. Create script to generate and insert data
def generate_data(num_records: int):
    fake = Faker()
    records = [
        PerformanceTestModel(
            name=fake.name(),
            value=random.random() * 1000,
            notes=fake.text(),
        ).dump_doc() for _ in range(num_records)
    ]
    
    # Use a direct pymongo client for bulk insert for speed
    collection = PerformanceTestModel.get_db_collection()
    collection.insert_many(records)
    print(f"Inserted {num_records} records into {collection.full_name}")

if __name__ == "__main__":
    generate_data(100_000)
```

#### A/B Benchmark Testing

To perform the A/B test, we need to temporarily keep the old `load_dataframe` logic accessible.

1.  **Preserve Legacy Method**: Before refactoring, rename the existing `load_dataframe` method to `_load_dataframe_legacy`.

    ```python
    # In loom/info/persist.py
    
    @classmethod
    def _load_dataframe_legacy(...) -> pd.DataFrame:
        # The original implementation
        with cls.aggregate(...) as cursor:
            df = pd.DataFrame(cursor)
            if "_id" in df.columns:
                df.set_index("_id", inplace=True)
            return df
    ```

2.  **Implement New Method**: Implement the new `load_dataframe` using the `pymongoarrow` strategy as detailed in the plan.

3.  **Create Benchmark Test**: Use the `timeit` module or a library like `pytest-benchmark` to compare the execution time of the two methods.

**Example Benchmark Test Sketch (`tests/test_performance.py`):**

```python
import timeit
from loom.info.persist import Persistable # Assuming PerformanceTestModel is accessible

def run_benchmark():
    # Ensure data exists before running
    
    # Time the legacy method
    legacy_time = timeit.timeit(
        "PerformanceTestModel._load_dataframe_legacy()",
        globals=globals(),
        number=10
    )

    # Time the new Arrow-based method
    arrow_time = timeit.timeit(
        "PerformanceTestModel.load_dataframe()",
        globals=globals(),
        number=10
    )

    print(f"Legacy implementation: {legacy_time:.4f} seconds")
    print(f"Arrow implementation:  {arrow_time:.4f} seconds")
    
    improvement = ((legacy_time - arrow_time) / legacy_time) * 100
    print(f"Improvement: {improvement:.2f}%")

if __name__ == "__main__":
    run_benchmark()
```

This testing strategy will provide concrete data on the performance gains from integrating `pymongoarrow` and validate the architectural changes.