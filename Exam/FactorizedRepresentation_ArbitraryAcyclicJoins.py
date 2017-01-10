#Question 2(b), 2(c) - Acyclic Joins

from IncDB import IncDB
from join import *
from functools import reduce
from operator import add
from collections import Counter, defaultdict

#Given a list of binary relation attributes and a list of joining attributes
#Returns the layout of the factorization-tree, ie. imagine R1(A,B), R2(A, C), R3(B,D)::
#         A: [B, C]
#         B: [D]
#and a list of weights of the joining attributes from which we can infer the order of the variables.
def TreeLayout(rel_attributes, jattributes):
    already_processed = set()
    fields = defaultdict(set)
    penalty = Counter()
    #Sort the attributes in descending order by the number of relations in which they are involved
    occurences = Counter([attr for rel in rel_attributes for attr in rel if attr in jattributes])
    order_of_attributes = sorted(occurences, key=occurences.get, reverse = True)
    #For each attribute
    for attr in order_of_attributes:
        for rel in rel_attributes:
            if attr not in rel:
                continue
            #For each relation containing the attribute "attr"
            #Obtain the name of the other attribute
            o_attr = rel[0] if rel[0] != attr else rel[1]
            #Check if it has not already been attached in the factorization-tree
            if o_attr not in already_processed:
                #Check if the other attribute is a joining attribute
                #and if it has not already been attached. If it has already been that it means that
                #the other attribute has attached the current attribute
                if o_attr in jattributes and o_attr not in penalty:
                    #Update the layout of the factorization tree by attacching
                    #other attribute as child of the current attribute
                    fields[attr].add(o_attr)
                    #Penalize the other attribute by penalty of the current attribute plus one
                    penalty.update({o_attr : penalty[attr] +1})
                    already_processed.add(attr)
                elif o_attr not in jattributes:
                    #otherwise it is not a joining attribute and we can attach it freely!
                    fields[attr].add(o_attr)
    #fields gives us the layout of the factorization tree
    #penalty gives us the order of the joining attributes (they are sorted by the height in the factorization-tree)
    return fields, penalty


#This represents a subtree
class FactorizedRepresentation_TreeNode:

    def __init__(self, joining_attribute, value):
        self.value = value
        #List containing the parents of this subtree
        self.parents = set()
        #The joining attribute at the root of this subtree
        self.joining_attribute = joining_attribute
        #The childs of the subtree
        self.childs = dict()

    #Check if the given supplied attribute is a child attribute of this subtree
    def __contains__(self, key):
        return key in self.childs

    #Returns a given attribute leaf
    def __getitem__(self, name):
        return self.childs[name]

    #Updates a child subtree
    def __setitem__(self, name, value):
        self.childs[name] = value

    def keys(self):
        return self.childs.keys()

    #This counts the number of results represented by the subtree
    def count(self):
        count = 1
        #For every child attribute
        for attr, child in self.childs.items():
            partial = 0
            #The child is a non-joining attribute so we just iterate over each
            #element
            if type(child) is set:
                for item in self[attr]:
                    partial += 1
            #Otherwise it is a joining attribute which means every child is a subtree so we
            #just call the apposite count method
            else:
                #For each subtree for the given attribute
                for key, subtree in child.items():
                    partial += subtree.count()
            #Update the partial product
            count *= partial
        return count


class FactorizedRepresentation_ArbitaryAcyclicJoins(IncDB):


    def __init__(self, database, jrels):
        IncDB.__init__(self, database, jrels)
        #Obtain the layout of the factorization-tree and of the
        #order of the joining variables
        self.fields, self.dominance = TreeLayout([database[rel][0] for rel in jrels], self.jattrs)
        #Initialize the two level hashtable attribute->valuie
        self.index = dict()
        for attr in self.jattrs:
            self.index[attr] = dict()
        #The materialized view
        self.view = list()
        for attr in [ a for a in self.jattrs if self.dominance[a] == 0 ]:
            self.view.append(self.index[attr])

        self.Fnode = FactorizedRepresentation_TreeNode


    #Given a set of tuples update the factorization-tree
    def fill(self, tuples):
        #The order of variables sorted in descending order by height of the attributes in
        #the factorization-tree
        attrs = sorted(self.jattrs, key= lambda x: self.dominance[x], reverse = True)
        #For ech tuples
        for t in tuples:
            #For each joining attribute starting from the ones at the bottom of the tree
            for attr in attrs:
                #Obtain the value of the attribute
                value = getattr(t, attr)
                #Obtain the child attributes of the given attributes
                fields = self.fields[attr]
                #Check whether a subtree for the current attribute with the given value 
                #already exists otherwise create it
                if value in self.index[attr]:
                    node = self.index[attr][value]
                else:
                    #Creation of a new subtree
                    node = self.Fnode(attr, value)
                    #Attach it to the factorization-tree
                    self.index[attr][value] = node
                #For each child attribute...
                for field in fields:
                    #...obtain the child attribute value
                    value = getattr(t, field)
                    #Deposith the value in the correct leaf of the subtree
                    if field in self.jattrs:
                        #If it is a joining attribute then we obtain
                        #the corresponding subtree
                        other_node = self.index[field][value]
                        #Then we update the list of pointers in the tree
                        if field not in node:
                            node[field] = dict()
                        node[field][value] = other_node
                        #We update the list of parent pointers
                        other_node.parents.add(node)
                    else:
                        if field not in node:
                            node[field] = set()
                        node[field].add(value)

    #Count query
    def count(self):
        count = 1
        #For every root attribute
        for subtrees in self.view:
            partial = 0
            #For every subtree for the current attribute
            for value, subtree in subtrees.items():
                #Count the number of results represented in current subtree and
                #update the sum
                partial += subtree.count()
            count *= partial
        #Return
        return count

    #Given a named tuple returns the joining attribute having greater height in the factorization tree
    def obtainJA(self, tuple):
        c = min([field for field in tuple._fields if field in self.jattrs], key = lambda x: self.dominance[x])
        return c


    def onInsert(self, relation, tuple):
        if relation not in self.jrels or self.existingRecord(relation, tuple):
            return
        #Most dominant joining attribute
        jattr = self.obtainJA(tuple)
        #Value of the most dominant joining attribute
        key = getattr(tuple, jattr)
        #The other attribute in the tuple
        other_attr = tuple._fields[0] if tuple._fields[0] != jattr else tuple._fields[1]
        #The tuple's other attribute's value
        other_value = getattr(tuple, other_attr)
        #If a given subtree with the given joining attribute's value exists
        if key in self.index[jattr]:
            node = self.index[jattr][key]
            #If the other attribute is a joining attribute
            if other_attr in self.jattrs:
                #If there exists a subtree with value of the other joining attribute...
                if other_value in self.index[other_attr]:
                    #...we can avoid a join query and update directly the materialized view
                    other_node = self.index[other_attr][other_value]
                    #Update the child subtree's parent list
                    other_node.parents.add(node)
                    #update the subtree child list with the pointer to the child subtree
                    node[other_attr][other_value] = other_node
                    #Terminate here
                    return
            #Otherwise it is not a joining attribute so just deposit the value in the correct leaf
            else:
                node[other_attr].add(other_value)
                #Terminate here
                return
        #If we reach this point we need to run the lightweight join query with the single-record relation trick
        new_rel = (self.db[relation][0], {tuple}, self.db[relation][2])
        rels = [self.db[rel] if rel != relation else new_rel for rel in self.jrels]
        res = multiway_join(*rels)
        if len(res[1]) > 0:
            self.fill(res[1])

    def onDelete(self, relation, tuple):
        if relation not in self.jrels or self.nonExistingRecord(relation, tuple):
            return
        #Most dominant joining attribute
        jattr = self.obtainJA(tuple)
        #Dominant joining attribute value
        key = getattr(tuple, jattr)
        #Other attribute
        other_attr = tuple._fields[0] if tuple._fields[0] != jattr else tuple._fields[1]
        #Other attribute value
        other_value = getattr(tuple, other_attr)
        #If there exists no subtree with the given joining attribute value then we are done
        #as it means that the record play no role in the result set of the conjunctive query.
        if key not in self.index[jattr]:
            return
        #If there exists a subtree...
        node = self.index[jattr][key]
        #..check if the other attribute is a non-joining attribute
        if other_attr not in self.jattrs:
            #...so just delete the value from the correct child of the subtree
            node[other_attr].discard(other_value)
        else:
            #Otherwise is a joining attribure so delete it from the list of pointers
            del node[other_attr][other_value]
        #Check if we emptied any leaf
        emptiedSomething = len(node[other_attr]) == 0
        if not(emptiedSomething):
            return
        #Queue of the nodes to visit
        toVisit = {node}
        #Blacklist (nodes to delete)
        toDelete = []
        #Obtain upward nodes to delete
        #1. Delete the all the links to che current subtree from parent nodes
        #2. If deleting the link to the current subtree empties any child of the parent nodes
        #   put the parent in the black list as it needs to be deleted.
        while len(toVisit) > 0:
            n = toVisit.pop()
            for other_node in n.parents:
                #Delete link to the subtree from the parent's node
                del other_node[n.joining_attribute][n.value]
                #Check if we emptied any child from the parent
                if len(other_node[n.joining_attribute]) == 0:
                    #If yes, then we need to put the parent in the queue and in the blacklist
                    toVisit.add(other_node)
                    toDelete.add(other_node)
        #Obtain downward nodes to delete
        #1. Delete all the links of a given node from its childs
        #2. Check if we empty any child's parent list
        #   as otherwise we need to put it in both the queue and blacklist
        toVisit = {node}
        while len(toVisit) > 0:
            n = toVisit.pop()
            for field in [key for key in n.keys() if key in self.jattrs]:
                for value, other_node in n[field].items():
                    other_node.parents.discard(n)
                    if len(other_node.parents) == 0:
                        toVisit.add(other_node)
                        toDelete.add(other_node)

        #Delete all the nodes in the blacklist
        for node in toDelete:
            del self.index[node.joining_attribute][node.value]
        #Delete the other original subtree referring to the deleted record's joining attribute value
        if key in self.index[jattr]:
            del self.index[jattr][key]

    def result(self):
        return self.view
