class Tuple:
    relation = None
    data = None

    def __init__(self, relation, data):
        self.relation = relation
        self.data = data

    def __getitem__(self, key):
        return self.data[self.relation.schema.index(key)]

class Relation:
    name = None
    schema = None
    data = None

    def __init__(self, name, schema):
        self.name = name
        self.schema = schema
        self.data = []

    def add(self, tup):
        self.data.append(tup)

    def __getitem__(self, key):
        return self.data[key]

    def __iter__(self):
        return iter(self.data)

    def __next__(self):
        return next(self.data)
