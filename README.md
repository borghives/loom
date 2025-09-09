# Loom

Weaves the fabric of information and time into one reality.

## North Star
A tapestry to which ideas are carried into reality swiftly, safely, and simply. The two most important concepts for such a reality are **information** and **time**.

## Philosophy

-   **Lower the mental load of coding**: Abstract away boilerplate and repetitive tasks, letting developers focus on business logic.
-   **Create friction for unsafe practices**: Make the safe way the easy way. The framework should guide developers away from common pitfalls, especially in data and time management.
-   **Promote simple, elegant code**: Prioritize clarity and simplicity over exhaustive features. We are willing to sacrifice capability for a simpler, more focused codebase. For example, Loom is intentionally built only for MongoDB to avoid the complexity of supporting multiple database paradigms.

## Philosophy in Action

Here’s how Loom's code reflects its core philosophy:

| Goal | Feature | How It Works |
| :--- | :--- | :--- |
| **Lower Mental Load** | **Declarative Persistence** (`Persistable`, `@declare_persist_db`) | Instead of writing database logic, you declare persistence by adding a mixin and a decorator to your Pydantic model. The framework handles the rest. |
| | **Automatic Timestamps & IDs** (`TimeInserted`) | Common fields like `created_at` is managed automatically through type annotations, reducing boilerplate and potential errors. |
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
@declare_persist_db(db_name="my_app", collection_name="users")
class User(Persistable):
    name: str
    email: str
    age: int

# 2. Create and save a new user
# The `_id`, `created_at`, and `updated_time` fields are handled automatically.
new_user = User(name="Alice", email="alice@example.com", age=30)
new_user.save() # Assuming a .save() method exists on the instance

# 3. Load a user from the database
retrieved_user = User.load_one(Filter({"email": "alice@example.com"}))
if retrieved_user:
    print(f"Found user: {retrieved_user.name}")

# 4. Load multiple users
active_users = User.load_many(Filter({"age": {"$lt": 40}}))
print(f"Found {len(active_users)} active users.")
```

### Core Concepts

-   **`Model` & `Persistable`**: Your data models inherit from `loom.info.model.Model` and mix in `loom.info.persist.Persistable`. This gives them database-aware capabilities.
-   **`@declare_persist_db`**: A class decorator that links your model to a specific MongoDB database and collection. It keeps your persistence logic clean and co-located with your data definition.
-   **Fluent API for Queries**: `Aggregation`, `Filter`, and `SortOp` provide a structured, chainable API to build complex database operations without writing raw MongoDB queries, making your code more readable and maintainable.
-   **Automatic Field Management**: Specialized types like `TimeInserted`, `TimeUpdated`, and `SuperId` automatically manage timestamps and `ObjectId` generation, reducing boilerplate and ensuring consistency.

### Initial limitations for simplicity:
- Support for MongoDB only.
- Secret management is handled via `google-cloud-secret-manager` or local `keyring`.

## Time

*Section to be developed.*