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
relations = [
    (data['lineitem'], 'l'),
    (data['orders'], 'o'),
    (data['customer'], 'c')
]
join_conditions= [
    ('c.custkey', 'o.custkey'),
    ('l.orderkey', 'o.orderkey')
]
algo = OnlineExploration  # set this to ExactWeight, ExtendedOlken, ExtendedOlkenAGM, OnlineExploration, etc.

print("Precomputing...")
st = time.time()
_, cache = algo(None, relations, {}, join_conditions)
print("Done. Precompute time = ", time.time() - st)
for i in range(10):
    print(i)
    time_start = time.time()
    print(chain_random_join(relations, algo, join_conditions, cache))
    print("Sample time = ", time.time() - time_start)

# Query X
algo = OnlineExploration
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
_, cache = algo(None, relations, {}, join_conditions)
print("Done.")
for i in range(10):
    print(i)
    time_start = time.time()
    print(chain_random_join(relations, algo, join_conditions, cache))
    print(time.time() - time_start)


# Query Y
#relations = [
#    (data['lineitem'], 'l2'),
#    (data['orders'], 'o2'),
#    (data['customer'], 'c2'),
#    (data['supplier'], 's'),
#    (data['customer'], 'c1'),
#    (data['orders'], 'o1')
#]
#join_conditions= [
#    ('o1.custkey', 'c1.custkey'),
#    ('l2.orderkey', 'o2.orderkey'),
#    ('o2.custkey', 'c2.custkey'),
#    ('c1.nationkey', 's.nationkey'),
#    ('s.nationkey', 'c2.nationkey'),
#]
#
#print("Precomputing weight...")
#_, cache = ExactWeightChain(None, relations, {}, join_conditions)
#print("Done.")
#for i in range(10):
#    time_start = time.time()
#    res = chain_random_join(relation, ExactWeightChain, join_conditions, cache)
#    
#    # reset relation schemas and key names
#    for table in data.keys():
#        for i, field in enumerate(data[table].schema):
#            data[table].schema[i] = data[table].schema[i].split('.')[-1]
#        if data[table].key is not None:
#            data[table].key = data[table].key.split('.')[-1]
#
#    rel = Relation('joined', ['orderkey', 'partkey'], None)
#    tup = Tuple(rel, (res[-1]['orderkey'], res[0]['partkey']))
#
#    semijoin_tuples = semijoin(tup, data['lineitem'], join_conditions=[('orderkey', 'orderkey'), ('partkey', 'partkey')])
#    M = len(semijoin_tuples)
#    if random.random() < (1 - len(semijoin_tuples) / M):
#        res = []
#    else:
#        res.append(random.choices(semijoin_tuples)[0])
#    
#    # reset relation schemas and key names
#    for table in data.keys():
#        for i, field in enumerate(data[table].schema):
#            data[table].schema[i] = data[table].schema[i].split('.')[-1]
#        if data[table].key is not None:
#            data[table].key = data[table].key.split('.')[-1]
#    print(time.time() - time_start)
