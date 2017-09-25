'''
A graph can be used to create streams and graph based processing.

Created on Sep 24, 2017

@author: aevans
'''


class Edge:

    def __init__(self, name, actor, edges, accountant, loop):
        self.name = name
        self.actor = actor
        self.edges = edges
        self.accountant = accountant
        self.loop = loop


class GraphBuilder:

    def __init__(self):
        edges = {}

    def connect_source(self, name, source, edges, accountant):
        pass

    def connect_edges(self, edge_map):
        pass

    def connect_edge(self, name, actor, edges, accountant, loop):
        pass

    def remove_edge(self, name, edge):
        pass

    def remove_edges(self, name_edge_map):
        pass

    def connect_sink(self, sink, edge):
        pass
