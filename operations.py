from containers import Tuple, Relation


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

    # Figure out which fields are common except 'comment'
    common_fields = []
    for field in t.relation.schema:
        if field != 'comment' and field in target_r.schema:
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
    return res

def W(t, target_rels):
    res = 0
    for tup in t:
        res += w(tup, target_rels)
    return res
