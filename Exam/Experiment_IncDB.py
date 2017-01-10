from FactorizedRepresentation_BinaryJoin import *
from relations import *
from join import *
from ArbitraryQueries_Evaluate import *

import copy, time, sys

iname = lambda x: "./Data/I" + str(x) + ".txt"
dname = lambda x: "./Data/D" + str(x) + ".txt"

db_instance = default_db_instance()

conj_query = sys.argv[1::]
conj_query = [rel for rel in conj_query if rel != "-count"]

test_count = "-count" in sys.argv

if len(conj_query) == 0:
    raise Exception("No conjunctive query specified")

time_limit = 60 * 60 # Set the total number of seconds
time_limit_reached = False

print("INC-DB")
print("CONJUNCTIVE QUERY", *["R"+ rel for rel in conj_query])
print("WITH COUNT" if test_count else "WITH NO COUNT")

IncDB_Engine = FactorizedRepresentation_BinaryJoin if len(conj_query) == 2 else obtainRepresentation([db_instance[rel] for rel in conj_query])

#Test Insertions
for i in range(1, 6):
    print("TESTING ON INSERTIONS FILE", i)
    db = copy.deepcopy(db_instance)
    initial_result = multiway_join(*[db[rel] for rel in conj_query])
    fdb = IncDB_Engine(db, conj_query)
    fdb.fill(initial_result[1])
    rows = 1
    elapsedTime = 0
    for line in open(iname(i)):
        els = line.split(",")
        relation = els[0]
        record = (int(els[1]), int(els[2]))
        record = db[relation][2](*record)
        start = time.time()
        fdb.onInsert(relation, record)
        elapsedTime += (time.time() - start)
        if test_count:
            start = time.time()
            fdb.count()
            elapsedTime += (time.time() - start)
        db[relation][1].add(record)
        rows += 1
        if elapsedTime > time_limit:
            time_limit_reached = True
            break

    if time_limit_reached:
        print("Could not process the whole file - ", "Time limit of", time_limit, "reached")
        break
    else:
        print("Processing ", i, "file took", elapsedTime)
    print("The query result has a total number of", fdb.count(), "rows")

time_limit_reached = False

for i in range(1, 6):
    print("TESTING ON DELETIONS FILE", i)
    db = copy.deepcopy(db_instance)
    initial_result = multiway_join(*[db[rel] for rel in conj_query])
    fdb = IncDB_Engine(db, conj_query)
    fdb.fill(initial_result[1])
    rows = 1
    elapsedTime = 0
    for line in open(dname(i)):
        els = line.split(",")
        relation = els[0]
        record = (int(els[1]), int(els[2]))
        record = db[relation][2](*record)
        start = time.time()
        fdb.onDelete(relation, record)
        elapsedTime += (time.time() - start)
        if test_count:
            start = time.time()
            fdb.count()
            elapsedTime += (time.time() - start)
        db[relation][1].discard(record)
        rows += 1
        if elapsedTime > time_limit:
            time_limit_reached = True
            break

    if time_limit_reached:
        print("Could not process the whole file - ", "Time limit of", time_limit, "reached")
        break
    else:
        print("Processing ", i, "file took", elapsedTime)
    print("The query result has a total number of", fdb.count(), "rows")