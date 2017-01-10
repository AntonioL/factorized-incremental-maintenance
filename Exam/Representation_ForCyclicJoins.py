#Question 2(b), 2(c) - Cyclic Joins

from collections import defaultdict, namedtuple
from IncDB import IncDB
from FactorizedRepresentation_BinaryJoin import *
from functools import reduce
from operator import mul

#Cyclic joins representation is a hybrid between inverted index and factorization-trees
#Inverted index take care of the cyclic part of the result set of the conjunctive query (the one involving all the join attributes)
#Factorization-trees take care of the acyclic part of the join if there is any

class Representation_ForCyclicJoins(IncDB):

    def __init__(self, database, jrels):
        IncDB.__init__(self, database, jrels)
        #The inverted index for the cyclic part of the result set
        self.index = dict()
        #Index for the subtree of the factorization-trees for the acyclic part
        #of the result set of the conjunctive query
        self.acyclic_fact_trees = dict()
        #A simple set for the cyclic part of the query
        self.cyclic_view = set()
        #Initialize inverted index
        for attr in self.jattrs:
            self.index[attr] = defaultdict(set)
        #This will contain the list of non-joining attributes paired indexed by the pairing joining attribute
        dependency = defaultdict(list)
        #This is a list of "acyclic relations" (=relations with just a single joining attribute)
        self.acyclic_rels = []
        for rel in jrels:
            if any(attr in self.otherAttrs for attr in database[rel][0]):
                self.acyclic_rels.append(rel)
                jattribute = database[rel][0][0] if database[rel][0][0] in self.jattrs else database[rel][0][1]
                dependency[jattribute].append(rel)
        #For each joining attribute that has some non-joining attribute dependencies
        #build an instance of the factorization-tree to store that.
        for attr in dependency:
            self.acyclic_fact_trees[attr] = FactorizedRepresentation_BinaryJoin(database, dependency[attr], attr)
        self.dependency = dependency
        self.cyclic_record = namedtuple('Record', self.jattrs)
        
    #Given a list of new result set tuples it updates the hybrid data structure
    def fill(self, tuples):
        for t in tuples:
            #Obtain the cyclic part of the current tuple
            sub_cyclic_record = self.cyclic_record(*[getattr(t, attr) for attr in self.jattrs])
            #Updates the inverted index with pointers to the "cyclic portion" (the portion containing joining attributes values) 
            #of the current result tuple
            #Check if the "cyclic portion" of the current result tuple already does not exist in the set
            if sub_cyclic_record not in self.cyclic_view:
                #For each joining attribute update the (joining_attribute, joining_attribute value) bucket
                #with pointer to the resultset tuple.
                for attr in self.jattrs:
                    key = getattr(t, attr)
                    index = self.index[attr][key]
                    index.add(sub_cyclic_record)
                self.cyclic_view.add(sub_cyclic_record)
            #Update the acyclic part of the query for every joining attribute
            #paired with non-joining attributes.
            #We delegate that to the factorization-tree implementation
            for rel in self.dependency:
                self.acyclic_fact_trees[rel].fill([t])


    def onInsert(self, relation, rec):
        if relation not in self.jrels or self.existingRecord(relation, rec):
            return
        #Check if the insert was done on a relation having a single joining attribute.
        if relation in self.acyclic_rels:
            #If that is the case...
            jattr = self.db[relation][0][0] if self.db[relation][0][0] in self.jattrs else self.db[relation][0][1]
            value = getattr(rec, jattr)
            #...check before if there exists a bucket (jattr, value) in the inverted index
            if value in self.index[jattr]:
                #...there exists! So we can simply update the factorization-tree by delegating that to
                #   its implementation!
                self.acyclic_fact_trees[jattr].onInsert(relation, rec)
                return

        #Otherwise either the relation has both the attributes being joining attributes
        #or there does not exist a bucket (jattr, value) in the inverted index
        #
        # anyway in both cases we need to run  a single-record relation join query
        new_rel = (self.db[relation][0], {rec}, self.db[relation][2])
        rels = [self.db[rel] if rel != relation else new_rel for rel in self.jrels]
        res = multiway_join(*rels)
        if len(res[1]) != 0:
            self.fill(res[1])

    #This purges the given result set "cyclic portion" tuples from the
    #hybrid data structure
    #deletedTuples is a list of the "cyclic portion" tuples to delete
    #purgeFactTree is a parameter which tells if we need to inspect the factorization-tree (and update it if necessary)
    #              if we empty a bucket in the inverted index.
    def purge(self, deletedTuples, purgeFactTree = True):
        #For each tuple
        for t in deletedTuples:
            #For each joining attribute in the conjunctive query
            for attr in self.jattrs:
                v = getattr(t, attr)
                #Remove the tuple from the inverted index bucket (attr, v)
                self.index[attr][v].remove(t)
                #If we emptied the bucket...
                if len(self.index[attr][v]) == 0:
                    #...remove that from the inverted index
                    del self.index[attr][v]
                    #...check if we need to remove that from the factorization-tree as well
                    #   if the current attribute is part of factorization-tree
                    if purgeFactTree and attr in self.acyclic_fact_trees:
                        del self.acyclic_fact_trees[attr].view[v]
            #Remove the tuple from the result set
            self.cyclic_view.remove(t)


    def onDelete(self, relation, rec):
        if relation not in self.jrels or self.nonExistingRecord(relation, rec):
            return
        #Check if the relation of the deleted tuple is cyclic (= attributes of the relations are both
        #joining attributes)
        if relation not in self.acyclic_rels:
            a,b = rec._fields
            x,y = rec
            index = self.index
            #Compute the intersection between the buckets in the result set
            deletedTuples = index[a][x].intersection(index[b][y])
            #Remove the tuples from the hybrid data structure
            self.purge(deletedTuples)
        #Otherwise if the relation has non-joining attributes..
        else:
            jattr = self.db[relation][0][0] if self.db[relation][0][0] in self.jattrs else self.db[relation][0][1]
            #...we are operating over factorization-trees so we delegate the deletion of that to the
            #factorization-tree implementation
            self.acyclic_fact_trees[jattr].onDelete(relation, rec)
            v = getattr(rec, jattr)
            #Check if we have emptied the subtree referring to the deleted record...
            if v not in self.acyclic_fact_trees[jattr].view:
                #...in which case we need to purge all the records falling in the inverted index bucket (jattr, v)
                deletedTuples = self.index[jattr][v].copy()
                self.purge(deletedTuples, False)

    #Count over the hybrid inverted-index factorization-tree representation
    def count(self):
        #1. Count over factorization-tree by creating a temporary 2-level hash-table of the form
        #   attribute -> value -> count
        acyclic_count = defaultdict(dict)
        #For every joining attribute involved in some relation with another
        #non-joining attribute
        for attr in self.dependency:
            #Obtain the factorization-tree for that attribute
            fact_tree =  self.acyclic_fact_trees[attr]
            #For every subtree...
            for value, node in fact_tree.view.items():
                #...multiply the number of the child attributes
                count_value = [ len(values) for _, values in node.items()]
                count_value = reduce(mul, count_value)
                #Update the temporary hash-table bucket (attr, value)
                acyclic_count[attr][value] = count_value
        #Iterate over the "cyclic" result set entry and count the number of combinations
        #having the entry itself as "prefix"
        count = 0
        for cyclic_result in self.cyclic_view:
            partial = 1
            #For each joining attribute involved in some relation with another
            #non-joining attribute
            for attr in self.dependency:
                value = getattr(cyclic_result, attr)
                #Update the partial count
                partial *= value
            #Update the global count
            count += partial
        return count

