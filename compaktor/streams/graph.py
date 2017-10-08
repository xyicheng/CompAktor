'''
A graph can be used to create streams and graph based processing.

Created on Sep 24, 2017

@author: aevans
'''


from compaktor.streams.managers.edge_manager import EdgeManager, Edge
from compaktor.streams.objects.accountant import AccountantActor
from compaktor.streams.managers.source_manager import SourceManager
from compaktor.streams.objects.source import Source
from compaktor.errors.actor_errors import WrongActorException
from compaktor.errors.graph_errors import GraphEdgeDoesNotExist
from compaktor.streams.objects.base_stage import BaseStage
from compaktor.message.message_objects import Subscribe, DeSubscribe, PoisonPill


class GraphManager:
    """
    Manage the graph edges
    """

    def __init__(self):
        self.__edge_manager = EdgeManager()
        self.__source_manager = SourceManager()
        self.accountant = AccountantActor()

    def connect_source(self, name, source, edges=[]):
        """
        Adds a source to the source manager and the graph.

        :param name: The name of the nodes for the source and edge managers
        :type name: str()
        :param source: The source actor
        :type source: Source()
        :param edges: The edges 
        """
        if isinstance(source, Source) is False:
            raise WrongActorException("Source must be supplied to Graph")
        if self.__source_manager.has_name(name):
            raise ValueError("Source is Already in the Manager")
        self.__source_manager.add_source(name, source)
        self.__edge_manager.add_edge_from_actor(name, source, edges)
        return self

    def add_node(self, start_name, actora, end_name=None):
        """
        Connect the edges of our graph.

        :param start_name: The start name for the graph
        :type start_name: str()
        :param actora: BaseActor()
        :type actora: BaseActor()
        :param end_name: The end name for the graph
        :type end_name: str()
        """
        keys = self.__edge_manager.keys()
        if start_name not in keys:
            err_msg = "Node {} Does Not Exist in Edge Connection".format(start_name)
            raise GraphEdgeDoesNotExist(err_msg)
        edgea = Edge(actora, [])
        edgeb = self.__edge_manager[end_name]
        actorb = edgeb.actor
        if end_name not in edgea.edges:
            edgea.edges.append(end_name)
            if isinstance(actora, BaseStage) or isinstance(actora, Source):
                asyncio.run_coroutine_threadsafe(actorb.tell(actora, Subscribe(actora)))

        if start_name not in edgeb.edges:
            edgeb.append(start_name)
        return self

    def connect_edge(self, start_name, end_name):
        """
        Connect edges from a start to end node.

        :param start_name: The name of the starting edge
        :type start_name: str()
        :param end_name: The name of the ending edge
        :type end_name: str()
        """
        keys = self.__edge_manager.keys()
        if start_name not in keys:
            err_msg = "Node {} Does Not Exist in Edge Connection".format(start_name)
            raise GraphEdgeDoesNotExist(err_msg)
        if end_name not in self.__source_manager.keys():
            err_msg = "Node {} Does Not Exist in Edge Connection".format(end_name)
            raise GraphEdgeDoesNotExist(err_msg)

        edgea = self.__edge_manager[start_name]
        edgeb = self.__edge_manager[end_name]
        actora = edgea.actor
        actorb = edgeb.actor
        if end_name not in edgea.edges:
            edgea.edges.append(end_name)
            if isinstance(actora, BaseStage) or isinstance(actora, Source):
                asyncio.run_coroutine_threadsafe(actorb.tell(actora, Subscribe(actora)))

        if start_name not in edgeb.edges:
            edgeb.append(start_name)
        return self

    def remove_edge(self, start_name, end_name):
        """
        Remove an edge

        :param start_name: The start edge name
        :type start_name: str()
        :param end_name: The end edge name
        :type end_name: str()
        """
        keys = self.__source_manager.keys()
        if start_name not in keys:
            err_msg = "Node {} Does Not Exist in Edge Connection".format(start_name)
            raise GraphEdgeDoesNotExist(err_msg)
        if end_name not in self.__source_manager.keys():
            err_msg = "Node {} Does Not Exist in Edge Connection".format(end_name)
            raise GraphEdgeDoesNotExist(err_msg)

        edgea = self.__edge_manager[start_name]
        edgeb = self.__edge_manager[end_name]
        actora = edgea.actor
        actorb = edgeb.actor
        if end_name not in edgea.edges:
            edgea.edges.append(end_name)

        if start_name not in edgeb.edges:
            edgeb.append(start_name)
            if isinstance(actora, BaseStage) or isinstance(actora, Source):
                asyncio.run_coroutine_threadsafe(actora.tell(actorb, DeSubscribe(actorb)))
        return self

    def close_graph(self, wait_time=10):
        """
        Shutdown the entire graph by iteratively sending poison pills to stages.

        :param wait_time: The time to wait for complete shutdown
        :type wait_time: int()
        """
        tick_actors = []
        # send poison pill for each source, errors should be ignored
        for node in self.__source_manager:
            actor = node.actor
            tick_actors.append(node.tick_actor)

        # wait for shutdown
        asyncio.get_event_loop().run_until_complete(asyncio.sleep(wait_time))

        # force stop nodes and publishers
        for node in self.__edge_manager:
            actor = node.actor
            asyncio.run_coroutine_threadsafe(actor.stop())
            if isinstance(actor, BaseStage) or isinstance(actor, Source):
                pub = actor.get_publisher()
                asyncio.run_coroutine_threadsafe(pub.stop())
        # force stop the tick actors
        for tick_actor in tick_actors:
            asyncio.run_coroutine_threadsafe(tick_actor.stop())
        return self
