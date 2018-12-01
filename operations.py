import time
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

def W(t, target_rels):
    res = 0
    for tup in t:
        res += w(tup, target_rels)
    return res


def chain_random_join(relations, W, join_conditions=None):
    t = None
    S = []
    W_current = W(t)

    for relation, name in relations:

        # Temporarily modify table schemas to accomodate for table names
        for i, field in enumerate(relation.schema):
            relation.schema[i] = name + '.' + field
        if relation.key is not None:
            relation.key = name + '.' + relation.key

        W_old = W_current
        
        semijoin_tuples = semijoin(t, relation, join_conditions=join_conditions)
        if len(semijoin_tuples) == 0:
            return []

        W_current = W(semijoin_tuples)
        if random.random() < (1 - W_current / W_old):
            return []
        t = random.choices(semijoin_tuples, weights=[W(t) for t in semijoin_tuples])[0]
        S.append(t)
        
    # Revert schema changes
    for relation, name in relations:
        for i, field in enumerate(relation.schema):
            relation.schema[i] = relation.schema[i].split('.')[1]
        if relation.key is not None:
            relation.key = relation.key.split('.')[1]

    print([t.data for t in S])
    return S
