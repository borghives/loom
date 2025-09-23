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
