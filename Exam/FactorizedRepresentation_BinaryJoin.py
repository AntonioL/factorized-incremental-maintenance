#Code for question 2 (a)
#also used in question 2 (b) for the acyclic part of the Representation for Cyclic joins

from IncDB import IncDB
from join import *
from functools import reduce
from operator import add


#Factored representation for a join on a single attribute (not necessarily binary)
class FactorizedRepresentation_BinaryJoin(IncDB):

    def __init__(self, database, jrels, jattrs = None):
        IncDB.__init__(self, database, jrels)

        #If the joining attribute is supplied then we use that
        if jattrs != None:
            self.jattrs = jattrs
            #In case a joining attribute is supplied it could be the case that the number of the relations
            #to be joined is 1 in which case the joining attribute would be put in the list of the normal attributes
            if jattrs in self.otherAttrs:
                self.otherAttrs = tuple(attr for attr in self.otherAttrs if attr != jattrs)
        #Otherwise check if the supplied relations have a joining attribute
        else:
            if len(self.jattrs) != 1:
               raise Exception('This class is meant for factored representation of joins involving a single attribute')

        #Given that the class is optimized for single attribute joins
        self.jattrs = self.jattrs[0]
        #THE MATERIALIZED VIEW!
        self.view = dict()

        #A list of non-joining attributes
        fields = self.otherAttrs

        #Class for representing a subtree
        class FR_TreeNode(dict):

            def __init__(self):
                for field in fields:
                    self[field] = set()

        self.Fnode = FR_TreeNode

    #Given a list of tuples representing a new result updates the materialized view
    def fill(self, tuples):
        for tuple in tuples:
            #Obtain the joining attribute's value
            k = getattr(tuple, self.jattrs)
            #If there exists no subtree with the given joining attribute value create it
            if k not in self.view:
                self.view[k] = self.Fnode()
            n = self.view[k]
            #For each non joining attribute...
            for field in self.otherAttrs:
                #...deposit the value in the correct child attribute leaf
                n[field].add( getattr(tuple, field) )

    def onInsert(self, relation, tuple):
        #The insert does not involve a relation involved in the conjunctive query to be materialized
        if relation not in self.jrels or self.existingRecord(relation, tuple):
            return
        #Value of the joining attribute
        key = getattr(tuple, self.jattrs)
        #Non joining attribute name
        other_attr = [ x for x in tuple._fields if x != self.jattrs ] [0]
        #Value of the joining attribute in the tuple
        value = getattr(tuple, other_attr)
        if key in self.view:
            #If there already exists a subtree deposit the non joining attribute value in the leaf.
            n = self.view[key]
            n[other_attr].add( value )
        else:
            #Otherwise run a single-record light-weight join query
            #1. Builds the single-record temporary relation
            new_rel = (self.db[relation][0], [tuple], self.db[relation][2])
            rels = [self.db[rel] if rel != relation else new_rel for rel in self.jrels]
            #2. Run the join query
            res = multiway_join(*rels)
            #Check whether there are new results to be put in the materialized view
            if len(res[1]) != 0:
                self.fill(res[1])

    def onDelete(self, relation, rec):
        #The delete does not involve a relation involved in the conjunctive query to be materialized
        if relation not in self.jrels or self.nonExistingRecord(relation, rec):
            return
        #Joining attribute's value
        key = getattr(rec, self.jattrs)
        #Other non-joining attribute
        other_attr = [ x for x in rec._fields if x != self.jattrs ] [0]
        #Value of the other non-joining attribute
        value = getattr(rec, other_attr)
        #Check if there already exists a sub-tree with the given joining attribute's value
        if key in self.view:
            n = self.view[key]
            #Delete from the corresponding leaf the non-attribute's value
            n[other_attr].discard(value)
            #Any empty child?
            if any(len(n[field]) == 0 for field in n.keys()):
                #If yes then remove the subtree
                self.view.pop(key)

    def count(self):
        count = 0
        #For every subtree in the current instance of the factorization-tree
        for value in self.view:
            #Subtree
            node = self.view[value]
            #Partial value
            partial = 1
            #The total number of results is given by the product of the number of elements in each leaf so
            #for each leaf of the subtree...
            for _, child in node.items():
                temp = 0
                #...for every element in the given leaf
                for y in child:
                    #...increase the counter by one
                    temp += 1
                #Update the partial product
                partial = partial * temp
            #Update the global count with the total number of results represented in the given subtree
            count += partial
        return count

    def result(self):
        return self.view

