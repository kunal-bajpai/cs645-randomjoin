import time
import numpy
import random
from containers import *


def join(r, s):
    """
    Join two Relation objects and return the resulting tuples in a new Relation.
    Name of new relation is <r.name>_<s.name>.
    """

    # Figure out which fields are common except 'comment'
    common_fields = []
    for field in r.schema:
        if field != 'comment' and field in s.schema:
            common_fields.append(field)
    
    # Build schema for result relation. Drop 'comment' field.
    schema = [f for f in list(r.schema) + list(s.schema) if f not in common_fields and f != 'comment']
    schema.extend(common_fields)

    result = Relation(r.name + '_' + s.name, tuple(schema))
    for outer in r:
        for inner in s:

            # Check if any common fields mismatch
            match = True
            for field in common_fields:
                if outer[field] != inner[field]:
                    match = False
                    break

            # If all match then add to result relation according to schema
            if match:
                joined = []
                for field in schema:
                    if field in r.schema:
                        joined.append(outer[field])
                    else:
                        joined.append(inner[field])
                result.add(Tuple(result, tuple(joined)))
    return result


def semijoin(t, target_r, join_conditions):
    """
    Return a list of tuples in target_r that join with t.
    """

    if t is None:
        if target_r.key is not None:
            return list(target_r.data.values())
        else:
            return target_r.data


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
    W_current = W(t, relations, cache, join_conditions)[0]

    for i, relation_tuple in enumerate(relations):
        relation, name = relation_tuple

        # Temporarily modify table schemas to accomodate for table names
        # This is needed for join conditions in semijoin to work.
        for i, field in enumerate(relation.schema):
            relation.schema[i] = name + '.' + field
        if relation.key is not None:
            relation.key = name + '.' + relation.key

        W_old = W_current
        
        semijoin_tuples = semijoin(t, relation, join_conditions=join_conditions)
        if len(semijoin_tuples) == 0:
            return []

        W_current = sum([W(t, [relations[ind] for ind in range(i+1, len(relations))], cache, join_conditions)[0] for t in semijoin_tuples])
        print("Rejecting with prob", W_current, W_old)
        if random.random() < (1 - W_current / W_old):
            return []
        weights=[W(t, [relations[ind] for ind in range(i+1, len(relations))], cache, join_conditions)[0] for t in semijoin_tuples]
        tot = sum(weights)
        t = numpy.random.choice(semijoin_tuples, p=[float(w/tot) for w in weights])
        S.append(t)
        
    # Revert schema changes
    for relation, name in relations:
        for i, field in enumerate(relation.schema):
            relation.schema[i] = relation.schema[i].split('.')[1]
        if relation.key is not None:
            relation.key = relation.key.split('.')[1]

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

def ExactWeightChain(t, target_rels, solutions, join_conditions):
    if t is not None and (t.relation.schema[0], t.data[0]) in solutions:
        return (solutions[(t.relation.schema[0], t.data[0])], solutions)
    if len(target_rels) == 0:
        answer = 1;
    else:
        relation, name = target_rels[0]
        for i, field in enumerate(relation.schema):
            relation.schema[i] = name + '.' + field.split('.')[-1]
        if relation.key is not None:
            relation.key = name + '.' + relation.key.split('.')[-1]
        results = semijoin(t,relation,join_conditions)
        answer = 0
        for result in results:
            answer = answer + ExactWeightChain(result,target_rels[1:], solutions, join_conditions)[0]
    if t is not None:
        solutions[(t.relation.schema[0], t.data[0])] = answer
    
    # Revert schema changes
    for relation, name in target_rels:
        for i, field in enumerate(relation.schema):
            relation.schema[i] = relation.schema[i].split('.')[-1]
        if relation.key is not None:
            relation.key = relation.key.split('.')[-1]
    return (answer, solutions)

def ExactWeightAcyclic(t, target_rel, solutions, join_conditions,graph):
    relation, name = target_rel
    for i, field in enumerate(relation.schema):
        relation.schema[i] = name + '.' + field.split('.')[-1]
    if relation.key is not None:
        relation.key = name + '.' + relation.key.split('.')[-1]
    if t is not None and (t.relation.schema[0], t.data[0],name) in solutions:
        return solutions[(t.relation.schema[0], t.data[0],name)];
    semijoin_tuples = semijoin(t, relation, join_conditions)
    if not graph[name]:
        solutions[(t.relation.schema[0], t.data[0],name)] = len(semijoin_tuples)
        return len(semijoin_tuples)
    answer = 0;
    for result in semijoin_tuples:
        product = 1;
        for child in graph[name]:
            product = product * ExactWeightAcyclic(result,child,solutions, join_conditions,graph)
        answer = answer + product;
    if t is not None:
        solutions[(t.relation.schema[0], t.data[0],name)] = answer
    return answer            
