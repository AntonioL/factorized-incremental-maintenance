from collections import namedtuple

#Function to read a file representing a relation
#It takes two attributes
#   fname = The path to the file to read
#   fields = The fields of the relation
#It returns a triple representing a relation of the form:
#   (FIELDS, TUPLES IN RELATION, FUNCTION)
#
#   FUNCTION is needed to decorate a tuple with relation's attributes
def relation_read_file(fname, fields):
    #A relation is a simple hash-table thus we get O(1) insertion, O(1) deletion and we can
    #also traverse it
    relation = set()
    #Builds the decorating function
    record = namedtuple('Record', fields)
    with open(fname) as f:
        for line in f:
            row = record(*[int(field) for field in line.strip().split(",")])
            relation.add(row)
    #Returns the relation
    return (fields, relation, record)

#This is the default database instance, it reads R1, R2, R3 and R4
#in the Data directory
def default_db_instance():
    #Database is represented through a hashtable of the form
    #   Relation Name -> Relation Data
    db = {}
    #The relation files have this structure Rx.txt with x being a number
    fname = lambda x : "./Data/R" + x + ".txt"
    db['1'] = relation_read_file(fname("1"), ("A", "B"))
    db['2'] = relation_read_file(fname("2"), ("A", "C"))
    db['3'] = relation_read_file(fname("3"), ("B", "C"))
    db['4'] = relation_read_file(fname("4"), ("A", "D"))
    return db