'''
Created on Sep 27, 2017

@author: aevans
'''


class Edge:

    def __init__(self, name, actor, edges, accountant, loop):
        self.name = name
        self.actor = actor
        self.edges = edges
        self.accountant = accountant
        self.loop = loop
