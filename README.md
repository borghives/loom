# Loom

Weaves the fabric of information and time into one reality.

## North Star
A tapestry to which ideas are carried into reality swiftly, safely, and simply.  The two most important concept for such reality is information and time.

## Goal
- Lower mental load of coding
- Create friction for writing unsafe code for two of the hardest concept information management and time
    - a developer can always write themselves into a deep pit.  Let do our best to make that harder
- Nice simple code.
    - willing to sacrifice capability for simpler code.  For example we might forever support only ONE database (mongodb). 
    - Feel free to clone to another Loom to support another database paradigm.  Since most likely the will complicate our code and increase the mental load of two different information management concept

## Non Goal / Anti Goal
This is not intended as a swiss army knife of coding framework.  The Loom capability will grow as needs grow.  Our tenet is to not over implement until needs arise.

## Information

The core of Loom's information management is a declarative and fluent data persistence layer built on top of Pydantic and MongoDB. The goal is to abstract away boilerplate code, allowing developers to focus on their application's business logic.

### Core Concepts

-   **Declarative Persistence**: At the heart of the system are Pydantic models. By inheriting from `PersistableModel`, your data models become database-aware. A simple decorator, `@declare_persist_db`, is used to link a model to a specific database and collection, keeping your persistence logic clean and co-located with your data definition.

-   **Fluent API**: Interacting with the database is done through a fluent, chainable API. This makes querying, aggregation, and data manipulation more readable and less error-prone. Classes like `Aggregation`, `Filter`, and `SortOp` provide a structured way to build complex database operations without writing raw MongoDB queries.

-   **Atomic Operations**: For scenarios requiring concurrent updates, the `IntCounter` type allows for atomic increment operations on integer fields, preventing race conditions and ensuring data consistency.

-   **Immutable Records**: The `LedgerModel` provides a non-destructive persistence option. Instead of updating documents, each save operation creates a new record, which is ideal for creating audit trails and immutable logs.

-   **Time-Series Data**: Loom has first-class support for time-series data. The `TimeSeriesLedgerModel` and `@declare_timeseries` decorator allow for easy configuration of time-series collections, including granularity and TTL settings. A suite of `TimeFrame` classes (e.g., `HourlyFrame`, `DailyFrame`) simplifies time-based querying and aggregation.

### Initial limitations for simplicity:
- support only local and remote mongodb database
- cloud secret are access through google-cloud-secret-manager and local keyring

## Time
