# Proposal: Enhance Filter Combination Logic

## 1. Analysis

The `loom.info` library provides a fluent interface for creating filters using the `fld` object and standard Python operators. However, there is a critical issue in how filters are combined using the `&` (AND) operator.

The current implementation merges the underlying dictionaries of the `Filter` objects. This works when the filters apply to different fields, but it fails when multiple conditions are applied to the **same field**.

For example, the expression:
```python
fld("age") > 30 & fld("age") < 40
```
Incorrectly results in a query that only includes the second condition (`< 40`), as it overwrites the first. The expected behavior is a query that finds documents where `age` is between 30 and 40. This bug makes the `&` operator unreliable and counter-intuitive for building complex filters.

## 2. Proposed Improvement

To resolve this, we will re-implement the filter combination logic to produce a correct logical `AND` operation that is compatible with MongoDB's query language.

### 2.1. Action Items

1.  **Fix Filter Combination Logic**: Modify the `Filter` class in `loom/info/filter.py` to correctly handle the `&` operator. Instead of performing a dictionary merge, the new implementation will create a MongoDB `$and` clause.

2.  **Implement Smart `$and` Handling**: The new logic will be capable of creating and extending `$and` clauses. If a filter already contains an `$and`, new conditions will be appended to it, avoiding unnecessary nesting and keeping the generated query clean.

### 2.2. Expected Behavior

With the new implementation, filter combination will work as follows:

- The expression `fld("age") > 30 & fld("age") < 40` will correctly generate a query equivalent to:
  ```json
  {"$and": [{"age": {"$gt": 30}}, {"age": {"$lt": 40}}]}
  ```

- Combining conditions on different fields will also use `$and` for consistency, producing a correct, if slightly more verbose, query:
  ```json
  {"$and": [{"age": {"$gt": 30}}, {"name": "John"}]}
  ```

### 2.3. Rationale

- **Correctness**: This change fixes a critical bug, ensuring that filter composition is reliable and behaves as expected.
- **Improved Ease of Use**: Developers will be able to intuitively and confidently chain multiple conditions using the `&` operator.
- **Robustness**: The filter-building API will become more robust and capable of handling more complex query scenarios.
