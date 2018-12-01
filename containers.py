KEY_FIELDS = ['partkey', 'suppkey', 'custkey', 'nationkey', 'orderkey', 'regionkey']

class Tuple:
    relation = None
    data = None

    def __init__(self, relation, data):
        self.relation = relation
        self.data = data

    def __getitem__(self, key):
        """
        name = Tuple['name']
        """
        return self.data[self.relation.schema.index(key)]

class Relation:
    name = None
    schema = None
    data = None
    key = None

    def __init__(self, name, schema, key=None):
        self.name = name
        self.schema = schema
        if key is None:
            self.data = []
        else:
            self.data = {}
        self.key = key

    def add(self, tup):
        if self.key is None:
            self.data.append(tup)
        else:
            self.data[tup[self.key]] = tup

    def __getitem__(self, key):
        return self.data[key]

    def __iter__(self):
        if self.key is not None:
            return iter(self.data.values())
        else:
            return iter(self.data)

    def __next__(self):
        if self.key is not None:
            return next(self.data.values())
        else:
            return next(self.data)
