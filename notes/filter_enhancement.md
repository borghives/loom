# Filter Combination Logic

## 1. Problem Analysis

The `loom.info` library provides a fluent interface for creating filters using the `fld` object and standard Python operators. The previous implementation had a critical issue in how filters were combined using the `&` (AND) and `|` (OR) operators.

The former implementation merged the underlying dictionaries of the `Filter` objects. This worked when the filters applied to different fields, but it failed when multiple conditions were applied to the **same field**.

For example, the expression:
```python
fld("age") > 30 & fld("age") < 40
```
Incorrectly resulted in a query that only included the second condition (`< 40`), as it overwrote the first. The expected behavior is a query that finds documents where `age` is between 30 and 40. This bug made the `&` operator unreliable and counter-intuitive for building complex filters.

## 2. Implemented Solution

To resolve this, the filter combination logic was re-implemented to produce correct logical `AND` and `OR` operations compatible with MongoDB's query language.

### 2.1. Implementation

The `Filter` class in `loom/info/filter.py` was modified to correctly handle the `&` and `|` operators. Instead of performing a dictionary merge, the new implementation creates a MongoDB `$and` or `$or` clause.

The logic is capable of creating and extending `$and` or `$or` clauses. If a filter already contains an `$and` or `$or`, new conditions are appended to it, avoiding unnecessary nesting and keeping the generated query clean.

Here is the implementation from `loom/info/filter.py`:
```python
# loom/info/filter.py

class Filter(Expression):
    # ... (other methods)

    def __and__(self, other):
        """
        Combines this filter with another using a logical AND.
        """
        if other is None:
            return self

        if not isinstance(other, Filter):
            other = Filter.wrap(other)

        if self.is_empty():
            return other
        if other.is_empty():
            return self

        self_clauses = self._value.data if isinstance(self._value, And) else [self]
        other_clauses = other._value.data if isinstance(other._value, And) else [other]
            
        return Filter(And(self_clauses + other_clauses))


    def __or__(self, other):
        """
        Combines this filter with another using a logical OR.
        """
        if other is None:
            return self

        if not isinstance(other, Filter):
            other = Filter.wrap(other)

        if self.is_empty():
            return other
        if other.is_empty():
            return self

        self_clauses = self._value.data if isinstance(self._value, Or) else [self]
        other_clauses = other._value.data if isinstance(other._value, Or) else [other]
                
        return Filter(Or(self_clauses + other_clauses))
```

### 2.2. Correct Behavior

With the new implementation, filter combination works as follows:

- The expression `fld("age") > 30 & fld("age") < 40` will correctly generate a query equivalent to:
  ```json
  {"$and": [{"age": {"$gt": 30}}, {"age": {"$lt": 40}}]}
  ```

- Combining conditions on different fields will also use `$and` for consistency, producing a correct, if slightly more verbose, query:
  ```json
  {"$and": [{"age": {"$gt": 30}}, {"name": "John"}]}
  ```

### 2.3. Benefits

- **Correctness**: This change fixed a critical bug, ensuring that filter composition is reliable and behaves as expected.
- **Improved Ease of Use**: Developers can intuitively and confidently chain multiple conditions using the `&` and `|` operators.
- **Robustness**: The filter-building API is now more robust and capable of handling more complex query scenarios.