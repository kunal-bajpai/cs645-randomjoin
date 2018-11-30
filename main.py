from containers import Relation, Tuple
from operations import join, semijoin, W

schema = {}

print("Reading data...")
with open("data/schema") as f:
    for row in f:
        tablename, fields = row.split(':')
        schema[tablename] = tuple(fields.split('|')[:-1])

tables = ['customer', 'lineitem', 'nation', 'orders', 'part', 'partsupp', 'region', 'supplier']
data = {}

for table in tables:
    data[table] = Relation(table, schema[table])
    with open("data/{}.tbl".format(table)) as f:
        for row in f:
            data[table].add(Tuple(data[table], tuple(row.split('|')[:-1])))

print("Done reading.")

print(join(data['part'], data['partsupp']))
