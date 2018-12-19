import time

from operations import *

# Query 3
def Q3(data, algo):
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
def QX(data, algo):
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


