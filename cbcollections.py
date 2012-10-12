class defaultdict(dict):

    """Poor man's implementation of defaultdict for Python 2.4
    """

    def __init__(self, default_factory=None, **kwargs):
        self.default_factory = default_factory
        super(defaultdict, self).__init__(**kwargs)

    def __getitem__(self, key):
        if self.default_factory is None:
            return super(defaultdict, self).__getitem__(key)
        else:
            try:
                return super(defaultdict, self).__getitem__(key)
            except KeyError:
                self[key] = self.default_factory()
                return self[key]
