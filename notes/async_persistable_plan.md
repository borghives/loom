# Plan: Adding Native Async Capability to `Persistable`

This document outlines the plan to introduce asynchronous database operations for `Persistable` objects using the native async support in `pymongo`. The approach is to add `async` counterparts to existing synchronous methods.

---

### **Phase 1: Core Infrastructure Setup**

1.  **Dependency:**
    *   No new dependency is required. Ensure the `pymongo` version is recent enough to include native async support (>=4.0).

2.  **Create Async Database Client Providers (`loom/info/universal.py`):**
    *   Implement `get_async_local_db_client()` and `get_async_remote_db_client()` using `pymongo.AsyncMongoClient`.
    *   These functions will mirror the existing synchronous logic for configuration and client caching.

---

### **Phase 2: Update `Persistable` and `LedgerModel` for Async Operations**

1.  **Modify `loom/info/persistable.py`:**
    *   **Async Getters**: Add methods to get an `AsyncMongoClient` and the corresponding async database and collection objects.
    *   **Async CRUD Methods**: Implement `persist_async()` and `persist_many_async()` using `await collection.find_one_and_update()`, etc.
    *   The `filter()` and `aggregation()` methods will continue to return the `LoadDirective`, which will now be a hybrid class.

2.  **Modify `loom/info/ledger.py`:**
    *   **`LedgerModel`**: Implement `persist_async()` and `persist_many_async()` using `await`.
    *   **`TimeSeriesLedgerModel`**: Implement `create_collection_async()`.

---

### **Phase 3: Augment `LoadDirective` for Hybrid Execution**

The existing `LoadDirective` will be modified to handle both synchronous and asynchronous execution.

1.  **Modify `loom/info/directive.py`:**
    *   Keep all existing pipeline-building methods (`filter`, `sort`, etc.) as they are.
    *   Add new `async` execution methods to the `LoadDirective` class.
        *   `load_aggregate_async()`: Will get an async `Collection` and iterate through its cursor using `async for`.
        *   `load_one_async()`, `load_many_async()`, `count_async()`, `exists_async()`: New async versions of the core data-loading methods.
    *   The existing synchronous methods (`load_aggregate`, `load_one`, etc.) will remain unchanged.

2.  **Omit `pymongoarrow` Functionality from Async:**
    *   No async counterparts for `load_table`, `load_dataframe`, or `load_polars` will be added. This functionality remains exclusively synchronous.
