# Improvement Report: Standardize `Aggregation` Class Method Naming

## 1. Problem: Inconsistent Naming Convention

The `loom.info.aggregation.Aggregation` class uses `PascalCase` for its methods (e.g., `Match`, `Group`, `Sort`). This is inconsistent with the standard Python PEP 8 style guide, which recommends `snake_case` for functions and methods.

This inconsistency reduces code clarity and predictability, especially when contrasted with other fluent builder classes in the same module, like `loom.info.directive.LoadDirective`, which correctly use `snake_case` (e.g., `filter`, `sort`, `limit`).

**Example of Inconsistency:**

- `aggregation.py`: `Aggregation().match(...).limit(...)`
- `directive.py`: `LoadDirective().filter(...).limit(...)`

## 2. Solution: Refactor to `snake_case`

To improve consistency and adhere to Python conventions, all public methods in the `Aggregation` class were renamed from `PascalCase` to `snake_case`.

## 3. Detailed Renaming

The following methods in `loom.info.aggregation.Aggregation` were renamed:

- `Match` -> `match`
- `Group` -> `group`
- `ReplaceRoot` -> `replace_root`
- `Project` -> `project`
- `AddFields` -> `add_fields`
- `Sort` -> `sort`
- `Limit` -> `limit`
- `Skip` -> `skip`
- `Unwind` -> `unwind`
- `Lookup` -> `lookup`
- `Merge` -> `merge`
- `Out` -> `out`
- `Sample` -> `sample`
- `GraphLookup` -> `graph_lookup`
- `Pipeline` -> `pipeline`

## 4. Execution Summary

The refactoring was executed as follows:

1.  **Renamed Methods:** The renames listed above were applied to the `Aggregation` class in `loom/info/aggregation.py`.
2.  **Updated Call Sites:** The entire `loom` project was searched for usages of the old `PascalCase` methods, and they were updated to the new `snake_case` names. The primary consumer identified and updated was the `LoadDirective` class in `loom/info/directive.py`.
3.  **Updated Documentation:** The `README.md` file was updated to reflect the new method names and to correct a syntactically incorrect example.

This report summarizes the changes that have been executed.

---

# Improvement Plan: Unify Query Directives

## 1. Analysis

The `loom/info` module provides a powerful and expressive interface for interacting with MongoDB. However, the presence of two distinct query-building classes, `LoadDirective` and `LoadDirectiveSimple`, introduces confusion.

- **`LoadDirective`**: Uses MongoDB's aggregation framework and is the primary, more powerful tool.
- **`LoadDirectiveSimple`**: Uses the basic `find` method. Its docstring mentions it exists "just in case there's something wrong with mongodb aggregate," which can undermine developer confidence.

This duality complicates the API and forces users to choose between two similar-looking tools without clear guidance.

## 2. Proposed Improvement

To improve ease of use and simplify the API, we will unify the query-building mechanism by removing `LoadDirectiveSimple`.

### 2.1. Action Items

1.  **Remove `LoadDirectiveSimple`**: Delete the `LoadDirectiveSimple` class from `loom/info/directive.py`.
2.  **Remove `find_simple` from `Persistable`**: Delete the `find_simple` class method from `loom/info/persistable.py`, as it is the entry point for `LoadDirectiveSimple`.
3.  **Standardize on `LoadDirective`**: `LoadDirective` will become the sole and standard way to build and execute queries. This provides a single, consistent, and powerful interface for all database read operations.

### 2.2. Rationale

- **Simplicity**: A single query path is easier to learn, use, and document.
- **Confidence**: Removing the backup `LoadDirectiveSimple` shows confidence in the aggregation-based approach, which is robust and highly optimized in modern MongoDB.
- **Maintainability**: The codebase will be cleaner and easier to maintain with the removal of redundant code.