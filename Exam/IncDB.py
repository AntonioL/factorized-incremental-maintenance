from collections import Counter

#This class is a backbone representing the IncDB's Materialized View engine
class IncDB:

    def __init__(self, database, jrels):
        #Database instance
        self.db = database
        #The joining relations for the supplied database instances
        #Defines the conjunctive query to be materialized
        self.jrels = jrels
        #Obtain the joining attributes of the conjunctive query
        self.jattrs = self.findAttributes([database[rel][0] for rel in jrels])
        #All the attributes of the conjunctive query
        self.attrs = set(attr for rel in jrels for attr in database[rel][0])
        #Non-joining attributes of the non conjunctive query 
        self.otherAttrs = tuple(self.attrs.difference(self.jattrs))
        #Some type-casting
        self.attrs = tuple(self.attrs)
        self.jattrs = tuple(self.jattrs)

    #Given a list of attributes of the relations returns the joining attributes
    def findAttributes(self, rel_attributes):
        #Initialize the counter
        c = Counter()
        #For every relation...
        for rel in rel_attributes:
            #...obtain the first attribute and increment it by one in the counter
            c.update({rel[0] : 1})
            #...obtain the second attribute and increment it by one in the counter
            c.update({rel[1] : 1})
        #Return all the attributes that occur more than once
        return [attr for attr in c if c[attr] > 1]

    #This function must be overriden from classes inheriting from IncDB
    #It updates the materialized view of the conjunctive query upon insertion of a record
    def onInsert(self, relation, tuple):
        pass

    #This function must be overriden from classes inheriting from IncDB
    #It updates the materialized view of the conjunctive query upon deletion of a record
    def onDelete(self, relation, tuple):
        pass

    #Updates the materialized view with a list of tuples
    def fill(self, tuples):
        pass

    #Count query
    #Counts the number of rows in the materialized view
    def count(self):
        pass

    #Checks whether a record exists in the current instance of a database
    def existingRecord(self, relation, record):
        return record in self.db[relation][1]

    #Checks whether a record does not exists in the current instance of a database
    def nonExistingRecord(self, relation, record):
        return self.existingRecord(relation, record) == False
