# Loom

Weaves the fabric of information and time into one reality.

A tapestry to which ideas are woven into reality swiftly, safely, and simply. The two most important concepts for such a reality are **information** and **time**.

## Philosophy

-   **Lower mental load**: Abstract away boilerplate and repetitive tasks, letting developers focus on business logic.
-   **Make the safe way the easy way**: Create friction for unsafe practices. The framework should guide developers away from common pitfalls, especially in data and time management.
-   **Promote simple, elegant code**: Prioritize clarity and simplicity over exhaustive features. We are willing to sacrifice capability for a simpler, more focused codebase. For example, Loom is intentionally built only for MongoDB to avoid the complexity of supporting multiple database paradigms.

## Philosophy in Action

Here’s how Loom's code reflects its core philosophy:

| Goal | Feature | How It Works |
| :--- | :--- | :--- |
| **Lower Mental Load** | **Declarative Persistence** (`Persistable`, `@declare_persist_db`) | Instead of writing database logic, you declare persistence by adding a mixin and a decorator to your Pydantic model. The framework handles the rest. |
| | **Automatic Timestamps & IDs** (`SuperId`, `TimeInserted`, `TimeUpdated`) | Common fields like `_id`, `created_at`, and `updated_time` are managed automatically through type annotations, reducing boilerplate and potential errors. |
| **Friction for Unsafe Code** | **Forced Secret Management** (`get_remote_db_client`) | The system requires a secret for remote database connections, preventing insecure configurations from the start. |
| | **Fluent Query API** (`Aggregation`, `Filter`) | Building queries with a structured, chainable API (`.Match()`, `.Group()`, etc.) is safer and less error-prone than writing raw MongoDB dictionaries. |
| **Simple, Elegant Code** | **Focused Design** | By targeting only MongoDB, the entire information management system remains lean, consistent, and easy to understand. |

## Information Management

The core of Loom's information management is a declarative and fluent data persistence layer built on top of Pydantic and MongoDB.

### Quick Start

Getting started with Loom is simple. Define your model, decorate it, and start interacting with the database.

```python
from loom.info.persist import Persistable, declare_persist_db
from loom.info.filter import Filter

# 1. Define a Pydantic model and make it Persistable
# The `_id`, `created_at`, and `updated_time` fields are handled automatically.
@declare_persist_db(db_name="my_app", collection_name="users")
class User(Persistable):
    name: str
    email: str
    age: int

# 2. Create and save a new user
new_user = User(name="Alice", email="alice@example.com", age=30)
new_user.persist()
print(f"Saved user with ID: {new_user.id}")

# 3. Load a user from the database using its ID
retrieved_user = User.from_id(new_user.id)
if retrieved_user:
    print(f"Found user: {retrieved_user.name}")

# 4. Update the user's age
retrieved_user.age = 31
retrieved_user.persist()
print(f"User's updated time: {retrieved_user.updated_time}")

# 5. Load multiple users with a filter
active_users = User.load_many(Filter({"age": {"$lt": 40}}))
print(f"Found {len(active_users)} active users.")

# 6. Create another user to persist multiple documents at once
another_user = User(name="Bob", email="bob@example.com", age=25)
User.persist_many([new_user, another_user])
```

### Core Concepts

-   **`Model` & `Persistable`**: Your data models inherit from `loom.info.model.Model` and mix in `loom.info.persist.Persistable`. This gives them database-aware capabilities.
-   **`@declare_persist_db`**: A class decorator that links your model to a specific MongoDB database and collection. It keeps your persistence logic clean and co-located with your data definition.
-   **Declarative Field Behavior**: Use annotations like `CoalesceOnInsert`, `CoalesceOnIncr`, and `QueryableTransformer` to declare how fields should behave during persistence and querying, further reducing boilerplate logic.
-   **Fluent API for Queries**: `Aggregation`, `Filter`, and `SortOp` provide a structured, chainable API to build complex database operations without writing raw MongoDB queries, making your code more readable and maintainable.
-   **Automatic Field Management**: Specialized types like `SuperId`, `TimeInserted`, and `TimeUpdated` automatically manage `ObjectId` generation and timestamps (`created_at`, `updated_time`), reducing boilerplate and ensuring consistency.
-   **Rich Querying and Loading**: Beyond simple `load_one` and `load_many`, Loom provides `load_latest`, `from_id`, `exists`, and a powerful `aggregate` method for complex data retrieval, including loading data directly into a pandas `DataFrame` with `load_dataframe`.
-   **Bulk Operations**: Use `persist_many` to efficiently save multiple model instances at once.

### Initial limitations for simplicity:
- Support for MongoDB only.
- Secret management is handled via `google-cloud-secret-manager` or local `keyring`.

## Time

*Section to be developed.*