'''
An Edge Manager

Created on Sep 28, 2017

@author: aevans
'''


from compaktor.wrappers.option import Option
from compaktor.actor.abstract_actor import AbstractActor


class Edge:

    def __init__(self, actor, edges):
        self.actor = actor
        self.edges = edges


class EdgeManager:
    """
    Manages the edges in a graph.

    The edge to 
    """

    def __init__(self):
        self.__edge_map = {}

    def add_edge(self, name, edge):
        """
        Add an edge to the map.

        :param name: The name of the edge
        :type name: str()
        :param edge: The edge to add
        :type edge: Edge()
        """
        if name in self.__edge_map.keys():
            self.__edge_map[name].edges.append(edge)
        else:
            self.__edge_map[name] = edge

    def add_edge_from_actor(self, name, actor, edges=[]):
        """
        Build and add an edge to the map.

        :param name; The name of the actor
        :type name: str()
        :param actor: The actor to add
        :type actor: AbstractActor()
        :param edges: The edge list to add.
        :type edges: list()
        """
        if isinstance(actor, AbstractActor) is False:
            raise ValueError("Reference for edge map must be actor")

        if isinstance(edges, list()) is False:
            raise ValueError("Edges Must be a List for the Edge Manager")

        if name in self.__edge_map.keys():
            edge = Edge(actor, edges)
            self.add_edge(name, edge)

    def edge_exists(self, namea, nameb):
        """
        Check if an edge exists

        :param namea: The name of the first edge in the map
        :type namea: str()
        :param nameb: The name of the second edge in the map
        :type nameb: str()
        """
        if namea in self.__edge_map.keys() and nameb in self.__edge_map.keys():
            return True
        return False

    def connect_edges(self, namea, nameb):
        """
        Add an edge to an existing edge list.

        :param namea: The name of the first edge
        :type namea: str()
        :param nameb: The name of the second edge
        :type nameb: str()
        """
