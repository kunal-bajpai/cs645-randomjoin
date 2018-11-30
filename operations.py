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


def semijoin(t, target_r):
    """
    Return a list of tuples in target_r that join with t.
    """

    curr_time = time.time()
    if t is None:
        return target_r.data

    # Figure out which fields are common except fields like anme and comment
    common_fields = []
    for field in t.relation.schema:
        if field in KEY_FIELDS and field in target_r.schema:
            common_fields.append(field)

    res = []
    for row in target_r:
        
        # Check if any common fields mismatch
        match = True
        for field in common_fields:
            if t[field] != row[field]:
                match = False
                break

        # If all match then add to result
        if match:
            res.append(row)
    print(time.time() - curr_time)

    return res

def W(t, target_rels):
    res = 0
    for tup in t:
        res += w(tup, target_rels)
    return res


def random_join(relations, W, join_conditions=None):

    t = None
    S = [None]
    W_current = W(t)

    for relation in relations:
        if type(target_r) is tuple:
            r = target_r[0]
            r.name = target_r[1]
            target_r = r

        W_old = W_current
        semijoin_tuples = semijoin(t, relation)
        W_current = W(semijoin_tuples)
        if random.random() < (1 - W_current / W_old):
            return [None]
        t = random.choices(semijoin_tuples, weights=[W(t) for t in semijoin_tuples])[0]
        S.append(t)
    print([t.data for t in S if t is not None])
    return S
