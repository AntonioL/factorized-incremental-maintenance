#Question 2(b), 2(c)
#Facilities for analyzing the conjunctive query
#and determine whether the join is cyclic or acyclic.
#Depending on whether its cyclic or acyclic we return
#the apposite representation.

from FactorizedRepresentation_ArbitraryAcyclicJoins import *
from Representation_ForCyclicJoins import *

#Given a list of relations to be joined
#it returns the representation for the result to be used
#depending on whether the conjunctive query
#is cyclic or acyclic
def obtainRepresentation(relations):
    cycles = find_cycles([rel[0] for rel in relations])
    return FactorizedRepresentation_ArbitaryAcyclicJoins if len(cycles) == 0 else Representation_ForCyclicJoins

#A graph algorithm to check the presence of cycles found on StackOverflow
#http://stackoverflow.com/questions/12367801/finding-all-cycles-in-undirected-graphs
def find_cycles(graph):
    cycles = []
    for edge in graph:
        for node in edge:
            findNewCycles([node], graph, cycles)
    #paths = [tuple(cy) for cy in cycles for node in cy]
    return [tuple(cycle) for cycle in cycles]

def findNewCycles(path, graph, cycles):
    start_node = path[0]
    next_node= None
    sub = []

    def invert(path):
        return rotate_to_smallest(path[::-1])

    #  rotate cycle path such that it begins with the smallest node
    def rotate_to_smallest(path):
        n = path.index(min(path))
        return path[n:]+path[:n]

    def isNew(path):
        return not path in cycles

    def visited(node, path):
        return node in path

    #visit each edge and each node of each edge
    for edge in graph:
        node1, node2 = edge
        if start_node in edge:
                if node1 == start_node:
                    next_node = node2
                else:
                    next_node = node1
        if not visited(next_node, path):
                # neighbor node not on path yet
                sub = [next_node]
                sub.extend(path)
                # explore extended path
                findNewCycles(sub, graph, cycles);
        elif len(path) > 2  and next_node == path[-1]:
                # cycle found
                p = rotate_to_smallest(path);
                inv = invert(p)
                if isNew(p) and isNew(inv):
                    cycles.append(p)