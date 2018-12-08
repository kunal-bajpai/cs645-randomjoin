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

def W(t, R=None):
    return 1

#start_time = time.time()
#
#print "Vignesh " + str(ExactWeightChain(None, [
#        (data['lineitem'], 'l'),
#        (data['orders'], 'o'),
#        (data['customer'], 'c')
#    ] , {},join_conditions=[('c.custkey', 'o.custkey'),
#        ('l.orderkey', 'o.orderkey')
#    ]))
#
#print time.time() - start_time

#start_time = time.time()
#
#print "Vignesh " + str(ExactWeightAcyclic(None, (data['lineitem'], 'l'), {},join_conditions=[
#            ('n.nationkey', 's.nationkey'),
#            ('s.nationkey', 'c.nationkey'),
#            ('c.custkey', 'o.custkey'),
#            ('o.orderkey', 'l.orderkey')
#        ], graph={
#            's': [(data['nation'], 'n')],
#            'c': [(data['supplier'], 's')],
#            'o': [(data['customer'], 'c')],
#            'n': [],
#            'l': [(data['orders'], 'o')]
#        }))
#print time.time() - start_time

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

print("Precomputing exact weight...")
_, cache = ExactWeightChain(None, relations, {}, join_conditions)
print("Done.")
for i in range(10):
    print(i)
    time_start = time.time()
    print(chain_random_join(
        relations,
        ExactWeightChain,
        join_conditions,
        cache
    ))
    print(time.time() - time_start)


# Query X
for i in range(10):
    time_start = time.time()
    acyclic_random_join(None, (data['lineitem'], 'l'), W, result=[],
        join_conditions=[
            ('n.nationkey', 's.nationkey'),
            ('s.nationkey', 'c.nationkey'),
            ('c.custkey', 'o.custkey'),
            ('o.orderkey', 'l.orderkey')
        ],
        graph={
            's': [(data['nation'], 'n')],
            'c': [(data['supplier'], 's')],
            'o': [(data['customer'], 'c')],
            'n': [],
            'l': [(data['orders'], 'o')]
        }
    )

    # reset relation schemas and key names
    for table in data.keys():
        for i, field in enumerate(data[table].schema):
            data[table].schema[i] = data[table].schema[i].split('.')[-1]
        if data[table].key is not None:
            data[table].key = data[table].key.split('.')[-1]

    print(time.time() - time_start)

# Query Y
for i in range(10):
    time_start = time.time()
    res = acyclic_random_join(None, (data['lineitem'], 'l2'), W, result=[], 
        join_conditions=[
            ('o1.custkey', 'c1.custkey'),
            ('l2.orderkey', 'o2.orderkey'),
            ('o2.custkey', 'c2.custkey'),
            ('c1.nationkey', 's.nationkey'),
            ('s.nationkey', 'c2.nationkey'),
        ],
        graph={
            'l2': [(data['orders'], 'o2')],
            'o2': [(data['customer'], 'c2')],
            'c2': [(data['supplier'], 's')],
            's': [(data['customer'], 'c1')],
            'c1': [(data['orders'], 'o1')],
            'o1': [],
        }
    )
    
    # reset relation schemas and key names
    for table in data.keys():
        for i, field in enumerate(data[table].schema):
            data[table].schema[i] = data[table].schema[i].split('.')[-1]
        if data[table].key is not None:
            data[table].key = data[table].key.split('.')[-1]

    rel = Relation('joined', ['orderkey', 'partkey'], None)
    tup = Tuple(rel, (res[-1]['orderkey'], res[0]['partkey']))

    semijoin_tuples = semijoin(tup, data['lineitem'], join_conditions=[('orderkey', 'orderkey'), ('partkey', 'partkey')])
    M = len(semijoin_tuples)
    if random.random() < (1 - len(semijoin_tuples) / M):
        res = []
    else:
        res.append(random.choices(semijoin_tuples)[0])
    
    # reset relation schemas and key names
    for table in data.keys():
        for i, field in enumerate(data[table].schema):
            data[table].schema[i] = data[table].schema[i].split('.')[-1]
        if data[table].key is not None:
            data[table].key = data[table].key.split('.')[-1]
    print(time.time() - time_start)
