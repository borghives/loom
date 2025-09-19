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

### 4. Dynamic Schema Generation (Revised)

The `@classmethod def get_arrow_schema(cls) -> pyarrow.Schema:` will be designed to be more robust.

-   It will inspect `cls.model_fields` to map Pydantic/Python types to `pyarrow` types.
-   It will correctly handle generic types like `Optional[T]` (by making the field nullable) and `list[T]`.
-   It will define `_id` as `pyarrow.string()`, as `ObjectId` is not a native Arrow type.

### 5. New & Updated Methods (Revised Architecture)

This revised architecture aligns with the project's philosophy of building public methods on top of more fundamental, lower-level ones.

-   **`@classmethod def aggregate_arrow(cls, aggregation: Aggregation, schema: Schema) -> pa.Table:`**
    - This is the new core primitive. It replaces the existing `aggregate` method for Arrow-based queries.
    - **Implementation:**
        1.  Get the `Collection` via `cls.get_db_collection()`.
        2.  Parse the `Aggregation` object into a pipeline via `cls.parse_agg_pipe(aggregation)`.
        3.  Execute the query using `pymongoarrow.aggregate.aggregate_arrow_all(collection, pipeline, schema=schema)`.
        4.  Return the resulting `pyarrow.Table`.

-   **`@classmethod def load_arrow_table(...) -> pyarrow.Table:`**
    - This will be the new public-facing method for Arrow-native workflows.
    - **Implementation:**
        1.  Construct the `Aggregation` object from `filter`, `sort`, and `sampling`, identical to the logic in the current `aggregate` method.
        2.  **Crucially, prepend a stage to the aggregation to convert `_id` to a string**: `Aggregation().AddFields(_id={"$toString": "$_id"})`. This makes the `_id` conversion explicit and predictable.
        3.  Generate the Arrow schema via `cls.get_arrow_schema()`.
        4.  Call `cls.aggregate_arrow(aggregation, schema)` and return the result.

-   **`@classmethod def load_dataframe(...) -> pd.DataFrame:` (Updated)**
    - This method's signature remains the same, providing a transparent performance upgrade.
    - **Implementation:**
        1.  Call `cls.load_arrow_table(...)` with all arguments.
        2.  Convert the resulting Arrow Table to a pandas DataFrame via `.to_pandas()`.
        3.  Return the DataFrame.

### 6. Example Implementation Sketch (Revised)

This sketch reflects the more modular, layered, and explicit approach.

```python
# In loom/info/persist.py

import pyarrow as pa
from pymongoarrow.api import Schema
from pymongoarrow.aggregate import aggregate_arrow_all
from typing import Optional, get_origin, get_args
# No monkey-patching, to keep the implementation explicit.

class Persistable(Model):
    # ... existing code ...

    @classmethod
    def get_arrow_schema(cls) -> Schema:
        """
        Generates a PyMongoArrow Schema from the Pydantic model fields.
        Handles Optional and list types.
        """
        type_map = {
            str: pa.string(),
            int: pa.int64(),
            float: pa.float64(),
            datetime: pa.timestamp("ns"),
        }

        fields = {}
        for name, field_info in cls.model_fields.items():
            field_type = field_info.annotation
            is_nullable = False

            origin = get_origin(field_type)
            if origin is Optional:
                is_nullable = True
                field_type = get_args(field_type)[0]
            
            # Add more complex type handling here (e.g., list, nested models)

            if field_type in type_map:
                fields[name] = type_map[field_type]

        # Override _id to always be a string, as it's converted in the pipeline
        fields["_id"] = pa.string()
        return Schema(fields)

    @classmethod
    def aggregate_arrow(cls, aggregation: Aggregation, schema: Schema) -> pa.Table:
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

        # Prepend _id conversion stage. This is explicit and ensures compatibility.
        id_conversion_stage = Aggregation().AddFields(_id={"$toString": "$_id"})
        final_aggregation = id_conversion_stage + aggregation

        if filter.has_filter():
            final_aggregation = final_aggregation.Match(filter)
        if sampling:
            final_aggregation = final_aggregation.Sample(sampling)
        final_aggregation = final_aggregation.Sort(sort)
        
        schema = cls.get_arrow_schema()
        return cls.aggregate_arrow(final_aggregation, schema)

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

    # ... rest of the class ...
```

### 7. Open Questions & Considerations

This remains an important step. The revised plan provides a stronger foundation for addressing these points.

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