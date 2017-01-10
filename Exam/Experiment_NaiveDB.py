from FactorizedRepresentation_BinaryJoin import *
from relations import *
from join import *

import copy, time, sys

iname = lambda x: "./Data/I" + str(x) + ".txt"
dname = lambda x: "./Data/D" + str(x) + ".txt"

db_instance = default_db_instance()

test_count = "-count" in sys.argv

conj_query = sys.argv[1::]
conj_query = [rel for rel in conj_query if rel != "-count"]

if len(conj_query) == 0:
    raise Exception("No conjunctive query specified")

time_limit = 60 * 60 # Set the total number of seconds
time_limit_reached = False

print("NAIVE-DB")
print("CONJUNCTIVE QUERY", *["R"+ rel for rel in conj_query])
print("WITH COUNT" if test_count else "WITH NO COUNT")

#Test Insertions
for i in range(1, 6):
    print("TESTING ON INSERTIONS FILE", i)
    db = copy.deepcopy(db_instance)
    elapsedTime = 0
    for line in open(iname(i)):
        els = line.split(",")
        relation = els[0]
        record = (int(els[1]), int(els[2]))
        record = db[relation][2](*record)
        db[relation][1].add(record)
        start = time.time()
        result = multiway_join(*[db[rel] for rel in conj_query], sort_merge = True)
        elapsedTime += (time.time() - start)
        if test_count:
            start = time.time()
            rows = 0
            for row in result[1]:
                rows += 1
            elapsedTime += (time.time() - start)
        if elapsedTime > time_limit:
            time_limit_reached = True
            break

    if time_limit_reached:
        print("Could not process the whole file - ", "Time limit of", time_limit, "reached")
        break
    else:
        print("Processing ", i, "file took", elapsedTime)
    rows = 0
    result = multiway_join(*[db[rel] for rel in conj_query], sort_merge = True)
    for row in result[1]:
        rows += 1
    print("The query result has a total number of", rows, "rows")

time_limit_reached = False

for i in range(1, 6):
    print("TESTING ON DELETIONS FILE", i)
    db = copy.deepcopy(db_instance)
    elapsedTime = 0
    for line in open(dname(i)):
        els = line.split(",")
        relation = els[0]
        record = (int(els[1]), int(els[2]))
        record = db[relation][2](*record)
        db[relation][1].discard(record)
        start = time.time()
        result = multiway_join(*[db[rel] for rel in conj_query], sort_merge = True)
        elapsedTime += (time.time() - start)
        if test_count:
            start = time.time()
            rows = 0
            for row in result[1]:
                rows += 1
            elapsedTime += (time.time() - start)
        if elapsedTime > time_limit:
            time_limit_reached = True
            break

    if time_limit_reached:
        print("Could not process the whole file - ", "Time limit of", time_limit, "reached")
        break
    else:
        print("Processing ", i, "file took", elapsedTime)
    rows = 0
    result = multiway_join(*[db[rel] for rel in conj_query], sort_merge = True)
    for row in result[1]:
        rows += 1
    print("The query result has a total number of", rows, "rows")