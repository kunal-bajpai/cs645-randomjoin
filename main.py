import copy

from containers import Relation, Tuple
from operations import *

schema = {}

print("Reading data...")
st = time.time()
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
    with open("../data/{}.tbl".format(table)) as f:
        for row in f:
            data[table].add(Tuple(data[table], tuple(row.split('|')[:-1])))

print("Done reading. Time to read = ", time.time() - st)

def W(t, R=None):
    return 1

# Query 3
def Q3(algo):
    relations = [
        (data['lineitem'], 'l'),
        (data['orders'], 'o'),
        (data['customer'], 'c')
    ]
    join_conditions= [
        ('c.custkey', 'o.custkey'),
        ('l.orderkey', 'o.orderkey')
    ]

    print("Precomputing...")
    st = time.time()
    _, cache = algo(None, relations, {}, join_conditions)
    print("Done. Precompute time = ", time.time() - st)
    for i in range(5):
        print(i)
        time_start = time.time()
        print(chain_random_join(relations, algo, join_conditions, cache))
        print("Sample time = ", time.time() - time_start)

# Query X
def QX(algo):
    relations = [
        (data['lineitem'], 'l'),
        (data['orders'], 'o'),
        (data['customer'], 'c'),
        (data['supplier'], 's'),
        (data['nation'], 'n')
    ]
    join_conditions= [
        ('n.nationkey', 's.nationkey'),
        ('s.nationkey', 'c.nationkey'),
        ('c.custkey', 'o.custkey'),
        ('o.orderkey', 'l.orderkey')
    ]

    print("Precomputing...")
    st = time.time()
    _, cache = algo(None, relations, {}, join_conditions)
    print("Done. Precompute time = ", time.time() - st)
    for i in range(5):
        print(i)
        time_start = time.time()
        print(chain_random_join(relations, algo, join_conditions, cache))
        print("Sample time = ", time.time() - time_start)

Q3(ExactWeightChain)
QX(ExactWeightChain)
Q3(OnlineExploration)
QX(OnlineExploration)
