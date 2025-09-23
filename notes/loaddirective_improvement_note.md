# Simplify `LoadDirective` by Unifying Queries under Aggregation Pipelines

**Summary:**
The `LoadDirective` class, which is central to building and executing database queries, currently maintains two separate internal representations for queries: one for simple `find` operations (filter, sort, limit) and another for complex aggregation pipelines. This proposal suggests refactoring `LoadDirective` to exclusively use MongoDB aggregation pipelines for all queries, simplifying its internal logic and improving maintainability.

**Problem:**
Currently, `LoadDirective` has a dual-state system. It manages `_filter_expr`, `_sort_expr`, and `_limit_expr` for basic queries, but switches to `_aggregation_expr` once an aggregation-specific method like `aggregation()` or `sample()` is called.

This leads to several issues:
-   **Increased Complexity:** Methods like `filter()`, `sort()`, and `limit()` must contain conditional logic to check if an aggregation is active (`if self._aggregation_expr is None:`).
-   **Redundant Code Paths:** Execution methods (`load_one`, `load_many`, `load_dataframe`, `exists`) need to implement two different paths: one using `find()` for simple queries and another using `aggregate()` for aggregation queries.
-   **Maintenance Overhead:** Adding new features or debugging is more difficult due to the need to consider both query paths and the transition between them. The `flatten_to_aggregation()` method is an example of glue code needed to bridge the two states.

**Proposed Solution:**
The refactoring will unify the query-building process by treating every query as an aggregation pipeline from the start.

1.  **Single Source of Truth:** Modify `LoadDirective` to use a single `_aggregation_expr: Aggregation` attribute as the sole representation of the query. Remove the now-redundant `_filter_expr`, `_sort_expr`, and `_limit_expr` attributes.

2.  **Initialize Aggregation:** In `LoadDirective.__init__`, initialize `self._aggregation_expr` to an empty `Aggregation()` object.

3.  **Simplify Builder Methods:** Update the query-building methods to directly append stages to the `_aggregation_expr` pipeline.
    -   `filter(f)` will become `self._aggregation_expr = self._aggregation_expr.Match(f)`
    -   `sort(s)` will become `self._aggregation_expr = self._aggregation_expr.Sort(s)`
    -   `limit(l)` will become `self._aggregation_expr = self._aggregation_expr.Limit(l)`
    -   `aggregation(agg)` will simply merge pipelines: `self._aggregation_expr |= agg`

4.  **Unify Execution Logic:** Remove the conditional branching in all execution methods. They will now always use the aggregation pipeline.
    -   `load_one()` will execute the pipeline with an appended `Limit(1)` stage.
    -   `load_many()` will execute the pipeline as is.
    -   `load_dataframe()` will always use `aggregate_pandas_all()`.
    -   `exists()` will execute the pipeline with `Limit(1)` and check if any document is returned.

5.  **Remove Helper Methods:** The `flatten_to_aggregation()` method will no longer be necessary and can be removed.

**Benefits:**
-   **Simplified Code:** The internal logic of `LoadDirective` will be significantly cleaner, with fewer states and no conditional branching for query construction and execution.
-   **Improved Maintainability:** A single, unified logic path makes the code easier to understand, debug, and extend.
-   **Consistency:** All database queries are handled through the powerful and general-purpose aggregation framework, removing the artificial distinction between "simple" and "complex" queries.
-   **Zero Impact on Public API:** This is a purely internal refactoring. The public-facing API for querying models (`YourModel.filter(...)`) will remain unchanged, ensuring no breaking changes for users of the library.

**Performance Verification (A/B Test):**
To address concerns about the potential performance impact of this change, a verification step will be added to the plan:

1.  **Create a Benchmark Test:** Before implementing the full refactoring, a dedicated performance test will be created.
2.  **A/B Comparison:** This test will perform an A/B comparison by executing a series of typical, simple queries (e.g., filter + sort + limit) using both:
    -   **A (Current Method):** The existing `find()`-based implementation in `LoadDirective`.
    -   **B (Proposed Method):** A modified version or separate function that uses an `aggregate()` pipeline for the same query.
3.  **Analyze Results:** The execution times for both methods will be measured and compared. The refactoring will only proceed if the performance of the aggregation-based approach is not significantly worse than the find-based approach.

This step will provide concrete data to ensure the simplification of `LoadDirective` does not come at the cost of performance for common query patterns.