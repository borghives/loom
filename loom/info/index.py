class Index:
    def __init__(self, keys, **kwargs):
        self.kwargs = kwargs
        self.kwargs["keys"] = keys

    def to_dict(self):
        return self.kwargs