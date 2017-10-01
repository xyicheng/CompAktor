'''
Manages sources in our graph

Created on Sep 30, 2017

@author: aevans
'''


import asyncio
import logging
from compaktor.streams.objects.source import Source
from compaktor.streams.objects.tick import TickActor
from compaktor.errors.actor_errors import WrongActorException, ActorStateError
from compaktor.state.actor_state import ActorState


class SourceNode:
    """
    The source node is stored in the manager for use later.
    """

    def __init__(self, name, source, tick_actor=TickActor()):
        """
        Constructor

        :param name: The name of the edge
        :type name: str()
        :param source: The source for the edge
        :type source: SourceActor()
        :param tick_actor: The tick actor for the source
        :type tick_actor: TickActor()
        """
        self.name = name
        self.source = source
        self.tick_actor = tick_actor


class SourceManager:
    """
        A source manager stores a map of SourceNodes.

        The source map stores source nodes.  Source nodes are structured as follows:
            - name
            - actor
            - tick_actor
    """

    def __init__(self):
        """
        Constructor
        """
        self.__source_map = {}

    def has_name(self, name):
        """
        Whether the source is already in the manager
        """
        return name in self.__source_map.keys()

    def add_source(self, name, source):
        """
        Add a source to the map

        :param name: The name of the source node
        :type name: str()
        :param source: The source to add
        :type source: Source()
        """
        if isinstance(source, Source):
            err_msg = "Must add Source actor to Source Manager."
            raise WrongActorException(err_msg)

        if source.get_stat() is not ActorState.RUNNING:
            source.start()

        if source.get_stat() is not ActorState.RUNNING:
            raise ActorStateError("Source Not Running")
        tick = TickActor()
        tick.start()
        source_node = SourceNode(name, source, tick)
        self.__source_map[name] = source_node

    def remove_source(self, name, stop_source=True):
        """
        Remove a source node

        :param name: The name of the source
        :type name: str()
        :param stop_source: Whether to stop the source
        :type stop_source: bool()
        :return: The SourceNode or None
        :rtype: SourceNode()
        """
        if name in self.__source_map.keys():
            edge = self.__source_map[name]
            actor = edge.actor
            tick_actor = edge.tick_actor
            if stop_source:
                if actor.get_state() is ActorState.RUNNING:
                    try:
                        asyncio.run_coroutine_threadsafe(actor.stop())
                    except Exception as e:
                        err_msg = "Failed to Stop Actor"
                        logging.warn(err_msg)

            if tick_actor.get_state() is ActorState.RUNNING:
                try:
                    asyncio.run_coroutine_threadsafe(tick_actor.stop())
                except Exception as e:
                    err_msg = "Failed to Stop Tick Actor for Source"
        return self.__source_map.pop(name, None)

    def get_source_by_name(self, name):
        """
        Get a source actor by name.

        :param name: The name of the node
        :type name: str()
        :return: The actor in the node or None
        :rtype: Source()
        """
        source_actor = None
        if name in self.__source_map.keys():
            source_edge = self.__source_map[name]
            source_actor = source_edge.actor
        return source_actor

    def get_source_node(self, name):
        """
        Get the source node from the map.

        SourceNode(name, actor, tick_actor)

        :param name: The name of the node
        :type name: str()
        :return: The SourceNode or None
        :rtype: SourceNode()
        """
        source_node = None
        if name in self.__source_map.keys():
            source_node = self.__source_map[name]
        return source_node
