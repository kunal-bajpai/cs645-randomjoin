import time
import numpy
import random
from containers import *


def read_data():
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
        with open("./data/{}.tbl".format(table)) as f:
            for row in f:
                data[table].add(Tuple(data[table], tuple(row.split('|')[:-1])))

    print("Done reading. Time to read = ", time.time() - st)

    return data



def semijoin(t, target_r, join_conditions):
    """
    Return a list of tuples in target_r that join with t.
    """

    if t is None:
        if target_r.key is not None:
            return list(target_r.data.values())
        else:
            return target_r.data
    #print(t.relation.name, t.relation.schema, target_r.name, target_r.schema, join_conditions)


    for cond in join_conditions:
        # Assuming that only one join condition will apply between two relations.
        # This is true for the 3 queries we are dealing with
        if cond[1] in t.relation.schema and cond[0] in target_r.schema:
            match_condition = (cond[1], cond[0])
            break
        if cond[0] in t.relation.schema and cond[1] in target_r.schema:
            match_condition = cond
            break

    res = []
    if match_condition[1] == target_r.key:
        try:
            res.append(target_r[t[match_condition[0]]])
        except IndexError:
            pass
    else:
        for row in target_r:
            if t[match_condition[0]] != row[match_condition[1]]:
                res.append(row)
    return res


def chain_random_join(relations, W, join_conditions, cache):
    t = None
    S = []

    # Temporarily modify table schemas to accomodate for table names
    # This is needed for join conditions in semijoin to work.
    for relation, name in relations:
        for i, field in enumerate(relation.schema):
            relation.schema[i] = name + '.' + field.split('.')[-1]
        if relation.key is not None:
            relation.key = name + '.' + relation.key.split('.')[-1]

    for i, relation_tuple in enumerate(relations):
        relation, name = relation_tuple

        st = time.time()
        W_old = W(t, relations, cache, join_conditions)[0]
        print("Time for first W call = ", time.time() - st)
        
        st = time.time()
        semijoin_tuples = semijoin(t, relation, join_conditions=join_conditions)
        print("Time for semijoin", time.time() - st)
        if len(semijoin_tuples) == 0:
            return []

        st = time.time()
        weights = [W(t, [relations[ind] for ind in range(i+1, len(relations))], cache, join_conditions)[0] for t in semijoin_tuples]
        print("Time for collecting weights = ", time.time() - st)
        st = time.time()
        W_current = sum(weights)
        print("Time for calculating sum of second W call = ", time.time() - st)
        print("Rejecting with prob", 1 - W_current/W_old)
        if i > 1 and random.random() < (1 - W_current / W_old):
            return []

        st = time.time()
        print("Number of tuples", len(semijoin_tuples))
        weights=[W(t, [relations[ind] for ind in range(i+1, len(relations))], cache, join_conditions)[0] for t in semijoin_tuples]
        tot = sum(weights)
        t = numpy.random.choice(semijoin_tuples, p=[float(w/tot) for w in weights])
        print("Time for random choice = ", time.time() - st)
        S.append(t)
       
    return S


def acyclic_random_join(t, R, W, join_conditions, graph, result):
    # Temporarily modify table schemas to accomodate for table names.
    # This is needed for join conditions in semijoin to work.
    relation, name = R
    for i, field in enumerate(relation.schema):
        relation.schema[i] = name + '.' + field.split('.')[-1]
    if relation.key is not None:
        relation.key = name + '.' + relation.key.split('.')[-1]

    semijoin_tuples = semijoin(t, relation, join_conditions)

    WR_old = W(t, relation)
    WR_current = W(semijoin_tuples)
    if random.random() < (1 - WR_current / WR_old):
        return -1
    
    t = random.choices(semijoin_tuples, weights=[W(t) for t in semijoin_tuples])[0]

    W_old = W(t)
    W_current = 1
    for child, child_name in graph[name]:
        W_current *= W(t, child)

    if random.random() < (1 - W_current / W_old):
        return -1

    result.append(t)
    for child in graph[name]:
        ret = acyclic_random_join(t, child, W, join_conditions, graph, result)
        if ret == -1:
            return -1

    return result
