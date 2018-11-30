import copy

from containers import Relation, Tuple
from operations import *

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

def W(t):
    return 1

#random_join([data['customer'], data['orders'], data['lineitem']], W)
#random_join([data['nation'], data['supplier'], data['customer'], data['orders'], data['lineitem']], W)
random_join([(data['lineitem'], 'l1'),
             (data['orders'], 'o1'),
             (data['customers'], 'c1'),
             (data['lineitem'], 'l2'),
             (data['orders'], 'o2'),
             (data['customers'], 'c2'),
             (supplier, 's')],
             W,
             join_conditions=[
                 ('l1.orderkey', 'o1.orderkey'),
                 ('o1.custkey', 'c1.custkey'),
                 ('l1.partkey', 'l2.partkey'),
                 ('l2.orderkey', 'o2.orderkey'),
                 ('o2.custkey', 'c2.custkey'),
                 ('c1.nationkey', 's.nationkey'),
                 ('s.nationkey', 'c2.nationkey'),
             ])
