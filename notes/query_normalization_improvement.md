# Proposal: Refactor Query Value Normalization

DECISION [FINAL]: DO NOT DO. while pushing the normalization closer to the operator wrapper class might make sense in other cases, for a well define Mongo query language it makes the logic fragmented for little "Robustness" benefit.  Since the normalization logic is federated to multiple operators class, I disagree on the cohesion argument.  However, this inspires a separated change which brings the normalization closer to the ModelFields and its QueryableField (major api breaking revision 2.6).  Read query_api_and_normalization_refactor.md for more info.

## 1. Analysis

The `loom/info/directive.py` module contains a mechanism for normalizing query values before they are sent to the database. This is driven by `NormalizeQueryInput` metadata on a `Model` and is implemented by a set of functions: `parse_agg_pipe`, `parse_filter_recursive`, and `transform_query_value`.

This implementation manually traverses the raw Python dictionary representing the MongoDB query to find and transform values.

This approach has several drawbacks:
- **Complexity and Brittleness**: The code for recursively parsing the query structure is complex and must be aware of all possible MongoDB operators (e.g., `$and`, `$or`, `$gt`). If a query uses an operator not explicitly handled by the parser, normalization will fail. This makes the system fragile.
- **Maintainability**: Maintaining this custom parser is difficult. Any changes to support new query patterns require modifying this centralized, complex logic.
- **Separation of Concerns**: The normalization logic is hidden within the `LoadDirective` and executed as a final step. This "magical" behavior is not transparent and is far removed from where the query filter is defined, making the system harder to reason about.

## 2. Proposed Improvement

To address these issues, the normalization logic should be refactored to be more object-oriented and robust, integrating it directly into the query expression classes.

### 2.1. Action Items

1.  **Introduce a `normalize` Method**: Add a new `normalize(self, normalizer_map: dict)` method to the `Expression` abstract base class (`loom/info/expression.py`). This method will be responsible for returning a new, normalized expression.

2.  **Implement `normalize` in Expression Subclasses**:
    -   In `QueryOpExpression` subclasses (`Gt`, `Lte`, `In`, etc.), the `normalize` method will apply the appropriate transformation function from the `normalizer_map` to its internal value.
    -   In container classes like `Filter`, `And`, and `Or`, the `normalize` method will delegate by recursively calling `normalize` on its child expression(s).

3.  **Refactor `LoadDirective`**:
    -   Remove the complex, manual parsing functions (`parse_agg_pipe`, `parse_filter`, `parse_filter_recursive`, `transform_query_value`) from `loom/info/directive.py`.
    -   Update `LoadDirective.get_pipeline_expr` to use the new new method. Before generating the final pipeline dictionary, it will call `filter.normalize(normalization_map)` on the `Filter` object within the `$match` stage.

### 2.2. Rationale & Benefits

- **Robustness**: This approach replaces fragile dictionary traversal with a well-defined, object-oriented visitor pattern. Each expression object knows how to normalize itself, making the system more resilient.
- **Simplicity & Maintainability**: The overall code will be simpler, cleaner, and easier to maintain. The complex parsing logic is replaced by smaller, more focused methods on each expression class.
- **Improved Cohesion**: The normalization logic is more closely tied to the expression objects it affects, improving code cohesion and making the system's behavior easier to understand.
