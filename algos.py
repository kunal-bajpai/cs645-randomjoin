from operations import semijoin


def ExactWeightChain(t, target_rels, solutions, join_conditions):
    try:
        return (solutions[(t.relation.schema[0], t.data[0])], solutions)
    except AttributeError:
        try:
            return (solutions["None"], solutions)
        except KeyError:
            pass
    except KeyError:
        pass

    if len(target_rels) == 0:
        answer = 1
    else:
        relation, name = target_rels[0]
        for i, field in enumerate(relation.schema):
            relation.schema[i] = name + '.' + field.split('.')[-1]
        if relation.key is not None:
            relation.key = name + '.' + relation.key.split('.')[-1]

        results = semijoin(t,relation,join_conditions)
        answer = sum([ExactWeightChain(result,target_rels[1:], solutions, join_conditions)[0] for result in results])
        
    try:
        solutions[(t.relation.schema[0], t.data[0])] = answer
    except AttributeError:
        solutions["None"] = answer
    
    
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

def ExtendedOlken(t, target_rels,solutions,join_conditions):
    if len(target_rels)==0:
        return 1
    relation, name = target_rels[0]
    if t is None:
        answer = len(relation.data)
    else:
        if relation.name in solutions:
            answer = findMaximumFrequency(relation)
        else:
            answer = solutions[relation.name]
        solutions[relation.name] = answer
    for relation,name in target_rels[1:]:
        if relation in solutions:
            answer = answer * solutions[relation.name]
        else:
            solutions[relation.name] = answer * findMaximumFrequency(relation)
            answer = solutions[relation.name]
    return (answer, solutions)

def findMaximumFrequency(relation):
    frequency = {}
    maximum = 0
    for tuple in relation.data:
        frequency[tuple[0]] = frequency.get(tuple[0], 0) + 1
    for k,v in frequency.items():
        if v>maximum:
            maximum = v
    return maximum

def ExtendedOlkenAGM(t,target_rels,solutions,join_conditions):
    if len(target_rels)==0:
        return 1
    answer = 1;
    for relation,name in target_rels:
        answer = answer * len(relation.data)
    return (answer, solutions)


def OnlineExploration(t,target_rels,solutions,join_conditions):
    try:
        return solutions[id(t)], solutions
    except KeyError:
        pass
    wanderEstimates = {}
    #Below check is to ensure we do not call OE after we have got results by performing random walks
    if len(solutions)>0:
        return ExactWeightChain(t, target_rels, solutions, join_conditions)
    for i in range(10):
        _, wanderEstimates = WanderChainJoinEstimates(t,target_rels,wanderEstimates,join_conditions)
    if id(t) in wanderEstimates:
        answer = sum(wanderEstimates[id(t)])/len(wanderEstimates[id(t)])
        solutions[id(t)] = answer
        return answer, solutions
    else:
        return ExactWeightChain(t, target_rels, solutions, join_conditions)

def WanderChainJoinEstimates(t,target_rels,solutions,join_conditions):
    if len(target_rels) == 0:
        answer = 1
    else:
        relation, name = target_rels[0]
        for i, field in enumerate(relation.schema):
            relation.schema[i] = name + '.' + field.split('.')[-1]
        if relation.key is not None:
            relation.key = name + '.' + relation.key.split('.')[-1]
        results = semijoin(t,relation,join_conditions)
        randomtuple = random.choice(results)
        answer = len(results)
        if answer!=0: 
             answer, _ = WanderChainJoinEstimates(randomtuple,target_rels[1:],solutions,join_conditions)
             answer = answer * len(results)
    solutions.setdefault(id(t), []).append(answer)
    return (answer, solutions)
