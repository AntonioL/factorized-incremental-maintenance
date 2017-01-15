from collections import *
from relations import *

FIELDS, RECORDS = 0, 1

#Implementation of a binary hash-join
#designed for IncDB
def h_binary_join(r1, r2):
    if len(r1[RECORDS]) > len(r2[RECORDS]):
        #The outer relation must the smallest one
        #so we create the hash-table of joining attributes
        #for the smallest relation
        tmp = r1
        r1 = r2
        r2 = tmp
    #The relation records
    r1_rel, r2_rel = r1[RECORDS], r2[RECORDS]
    #Descriptors containing the relations attribute names
    r1_fields = set(r1[FIELDS])
    r2_fields = set(r2[FIELDS])
    #Obtain the joining attributes of the to relations to be joined
    joining_attributes = r1_fields.intersection(r2_fields)
    #List of the attributes in the result
    result_attributes = tuple(r1_fields.union(r2_fields))
    #A functon to add named attributes to tuple records so we can access
    #them by their attribute's name
    record = namedtuple('Record', result_attributes)
    #The relation holding the result of the query
    result_relation = set()
    #Buckets creation of the outer relation
    buckets = defaultdict(list)
    for rec in r1_rel:
        j_attr = tuple(getattr(rec, attr) for attr in joining_attributes)
        buckets[j_attr].append(rec)
    #Iterate through the every record in the smaller relation
    for r2_rec in r2_rel:
        #Obtain the values of the joining attributes
        j_attr = tuple(getattr(r2_rec, attr) for attr in joining_attributes)
        #Check if there is match
        if j_attr in buckets:
            #There is matching bucket for the joining attribute of the inner relation
            #Iterates every element falling in the bucket and at every iteration
            #creates a new result record
            for r1_rec in buckets[j_attr]:
                #Make new result record
                fields = [getattr(r1_rec, attr) if hasattr(r1_rec, attr) else getattr(r2_rec, attr) for attr in result_attributes]
                #Adds that to the result relation
                result_relation.add(record(*fields))
    #Returns the relation holding the result
    return (result_attributes, result_relation, record)

#Implementation of a binary hash-join
#designed for NaiveDB
def sm_binary_join(r1, r2):
    if len(r1[RECORDS]) > len(r2[RECORDS]):
        #The outer relation must the smallest one
        #so we create the hash-table of joining attributes
        #for the smallest relation
        tmp = r1
        r1 = r2
        r2 = tmp
    #The records inside both the relations
    r1_rel, r2_rel = r1[RECORDS], r2[RECORDS]
    #Descriptor containing the field names
    r1_fields = set(r1[FIELDS])
    r2_fields = set(r2[FIELDS])
    #The attribute on which the join is going to performed
    joining_attributes = r1_fields.intersection(r2_fields)
    #Result
    result_attributes = tuple(r1_fields.union(r2_fields))
    record = namedtuple('Record', result_attributes)
    result_relation = set()
    #Function to obtain the joining attribute value of a record
    j_attr_value = lambda x: [getattr(x, attr) for attr in joining_attributes]
    #Sort_Phase
    r1_sorted = sorted(r1[RECORDS], key=j_attr_value)
    r2_sorted = sorted(r2[RECORDS], key=j_attr_value)
    #Merge-phase
    r1_idx = 0
    r2_idx = 0
    #Until we have iterated every record of one of the two relations
    while r1_idx < len(r1_sorted) and r2_idx < len(r2_sorted):
        #The records of the two relations
        r1_rec = r1_sorted[r1_idx]
        r2_rec = r2_sorted[r2_idx]
        #The joining attribute of the two records
        r1_j = j_attr_value(r1_rec)
        r2_j = j_attr_value(r2_rec)
        #We do not have a match
        #so we increase the row number of the relation
        #having the smallest value for the joining attribute
        if r1_j > r2_j:
            r2_idx += 1
        elif r1_j < r2_j:
            r1_idx += 1
        #We have a match
        else:
            #The record to be added in the result
            fields = [getattr(r1_rec, attr) if hasattr(r1_rec, attr) else getattr(r2_rec, attr) for attr in result_attributes]
            #Update the result
            result_relation.add(record(*fields))
            #Auxiliary counter to the second relation
            #There could be another match through the second relation so
            #we continue iterating and check if there is any
            i = r2_idx
            #Until we iterate every record...
            while i < len(r2_sorted):
                r2_rec = r2_sorted[i]
                r2_j = j_attr_value(r2_rec)
                #We have a match
                if r1_j == r2_j:
                    #Add the record to the result and increments the counter of the second relation
                    #It handles automatically duplicates as result is represented as a set
                    fields = [getattr(r1_rec, attr) if hasattr(r1_rec, attr) else getattr(r2_rec, attr) for attr in result_attributes]
                    result_relation.add(record(*fields))
                    i += 1
                else:
                    break
            #Increase the counter of the first relation
            r1_idx += 1
    #Return the result
    return (result_attributes, result_relation, record)

#Function to execute multi-join
#The parameters are:
#   - The relations to be joined
#   - The join method to use (True = hash-join, False = sort-mergejoin)
def multiway_join(*relations, sort_merge=False):
    #Sort the relations from the smallest one to the biggest one
    sorted_relations = iter(sorted(relations, key= lambda rel: len(rel[RECORDS])))
    #Current semi-join result accumulator
    semi_join = next(sorted_relations)
    join_method = sm_binary_join if sort_merge else h_binary_join
    #Execute a series of semi-joins
    for rel in sorted_relations:
        semi_join = join_method(semi_join, rel)
    return semi_join