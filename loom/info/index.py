class Index:
    """
    A data class for defining a MongoDB index.

    This class is used to specify indexes in the `@declare_persist_db` decorator
    for a `Persistable` model.
    """
    def __init__(self, keys, **kwargs):
        self.kwargs = kwargs
        self.kwargs["keys"] = keys

    def to_dict(self):
        return self.kwargs