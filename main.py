import copy

from containers import Relation, Tuple
from operations import *

schema = {}

print("Reading data...")
with open("data/schema") as f:
    for row in f:
        tablename, fields = row.split(':')
        schema[tablename] = fields.split('|')[:-1]

tables = [('customer', 'custkey'),
          ('lineitem', None),
          ('nation', 'nationkey'),
          ('orders', 'orderkey'),
          ('part', 'partkey'),
          ('partsupp', None),
          ('region', 'regionkey'),
          ('supplier', 'suppkey')]
data = {}

for table, key in tables:
    data[table] = Relation(table, schema[table], key=key)
    with open("data/{}.tbl".format(table)) as f:
        for row in f:
            data[table].add(Tuple(data[table], tuple(row.split('|')[:-1])))

print("Done reading.")

def W(t):
    return 1

# Q3
for i in range(100):
    chain_random_join(
    [
        (data['lineitem'], 'l'),
        (data['orders'], 'o'),
        (data['customer'], 'c')
    ],
    W,
    join_conditions=[
        ('c.custkey', 'o.custkey'),
        ('l.orderkey', 'o.orderkey')
    ]
    )


# QX
#random_join(
#    [
#        (data['nation'], 'n'),
#        (data['supplier'], 's'),
#        (data['customer'], 'c'),
#        (data['orders'], 'o'),
#        (data['lineitem'], 'l')
#    ],
#    W,
#    join_conditions=[
#        ('n.nationkey', 's.nationkey'),
#        ('s.nationkey', 'c.nationkey'),
#        ('c.custkey', 'o.custkey'),
#        ('o.orderkey', 'l.orderkey')
#    ]
#)
#
##QY
#random_join(
#    [
#        (data['lineitem'], 'l1'),
#        (data['orders'], 'o1'),
#        (data['customer'], 'c1'),
#        (data['lineitem'], 'l2'),
#        (data['orders'], 'o2'),
#        (data['customer'], 'c2'),
#        (data['supplier'], 's')
#    ],
#    W,
#    join_conditions=[
#        ('l1.orderkey', 'o1.orderkey'),
#        ('o1.custkey', 'c1.custkey'),
#        ('l1.partkey', 'l2.partkey'),
#        ('l2.orderkey', 'o2.orderkey'),
#        ('o2.custkey', 'c2.custkey'),
#        ('c1.nationkey', 's.nationkey'),
#        ('s.nationkey', 'c2.nationkey'),
#    ]
#)
