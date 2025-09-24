# Proposal: Model-Centric Query Fields

TL;DR: DO NOT DO.  Static checker like pylance was not able to discover Model's fields. Implementation unsuccessful Due to Pydantic conflict?  The code became more complex then necessary.

## 1. Analysis

The current query-building mechanism in `loom.info` relies on a generic `fld("field_name")` helper function to create filter expressions. While functional, this approach has several usability drawbacks:

- **Discoverability**: New users must learn about the `fld` helper, as its use is not immediately obvious from the `Model` or `Persistable` APIs.
- **Decoupling**: Field names are passed as strings (`fld("age")`), which decouples the query from the model definition. This is prone to runtime errors from typos and is not supported by static analysis or IDE autocompletion.
- **Readability**: Queries are less intuitive as the fields are not directly associated with the model they belong to, for example `MyModel.filter(fld("age") > 30)`.

## 2. Proposed Improvement

To enhance clarity, usability, and safety, we will refactor the query-building process to be model-centric. The goal is to enable developers to build queries by accessing queryable field objects through a dedicated class method on the `Model`.

**Current:**
```python
from loom.info.field import fld
my_filter = fld("age") > 30
```

**Proposed:**
```python
from my_models import User
my_filter = User.fields().age > 30
```

### 2.1. Implementation Plan

1.  **Create `QueryableField` Class**: A new `QueryableField` class will be created to contain the logic currently in the `fld` helper. This class will generate `Filter` objects when comparison operators (`==`, `>`, `<`, etc.) are used on its instances.

2.  **Implement `Model.fields()` Class Method**: The base `loom.info.model.Model` class will be modified to include a `classmethod` named `fields`.
    - This method will act as an explicit entry point for building queries.
    - When called (e.g., `User.fields()`), it will dynamically create and return a lightweight container object.
    - This container will have attributes that mirror the model's fields (e.g., `name`, `age`), where each attribute is an instance of `QueryableField`.
    - The generated container will be cached on the model class to ensure it is only created once.

3.  **Deprecate `fld`**: The `fld` helper will become redundant and can be deprecated and eventually removed from the public API, simplifying the library.

### 2.2. Rationale & Benefits

- **Improved Usability & Explicitness**: This pattern is highly discoverable and makes the intent of building a query filter explicit. It avoids the "magic" of modifying the class via `__init_subclass__`.
- **Enhanced Readability**: Queries will be clearer and more self-documenting (e.g., `User.filter(User.fields().age > 30)`).
- **Increased Safety**: Using the fields container allows static analysis tools and IDEs to provide autocompletion and catch errors from typos in field names before runtime.
- **Better Discoverability**: The query mechanism becomes an integral, explicit part of the `Model` API, making it easier for new users to discover and learn.

### 2.3. Update: Static Typing and `__init_subclass__`

While the initial proposal aimed to avoid the use of `__init_subclass__`, further investigation revealed that it is the most effective mechanism for providing robust static type hinting for the `fields()` method.

To enable IDEs and static analysis tools like Pylance to provide autocompletion and catch errors, the type of the object returned by `fields()` must be known statically. The implemented solution uses `__init_subclass__` to dynamically generate a unique, type-annotated `fields` method for each `Model` subclass.

This approach achieves the goal of "Increased Safety" outlined in the proposal in a way that would not be possible with a more dynamic, runtime-only approach. The `__init_subclass__` magic is therefore embraced as a pragmatic solution to a difficult problem in Python's type system, prioritizing developer experience and code safety.
