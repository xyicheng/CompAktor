'''
Created on Sep 21, 2017

@author: aevans
'''

import asyncio
from compaktor.structure.queue import BlockingQueue 
from compaktor.actor.base_actor import BaseActor
from compaktor.errors.actor_errors import ActorStateError
from compaktor.message.message_objects import RouteAsk,\
    QueryMessage, RouteBroadcast, RouteTell, Subscribe
from compaktor.registry import actor_registry as registry
from compaktor.state.actor_state import ActorState
from compaktor.utils.name_utils import NameCreationUtils
from random import random
import logging
from compaktor.actor.abstract_actor import AbstractActor


class BalancingRouter(BaseActor):
    """
    The balancing router uses a single message queue to route messages to
    actors.  Every actor shares the common queue.  This requires reworking
    the actors to allow a special blocking queue.
    """

    def __init__(self, name=None, loop=None, address=None, mailbox_size=10000,
                 inbox=None, actors=[]):
        if name is None:
            name = NameCreationUtils.get_name_base()
            name = NameCreationUtils.get_name_and_number(str(name))
        if address is None:
            address = name
        super().__init__(name, loop, address, mailbox_size, inbox)
        if inbox is not None:
            self.__queue = inbox
        self.__queue = asyncio.Queue(maxsize=mailbox_size)
        self.__router_queue = BlockingQueue(max_size=mailbox_size)
        self.actor_set = actors
        self.set_router_handlers()
        self.actor_system = None
        self.sys_path = None

    def set_router_handlers(self):
        """
        Set the router handlers
        """
        self.register_handler(RouteAsk, self.route_ask)
        self.register_handler(RouteTell, self.route_tell)
        self.register_handler(RouteBroadcast, self.attempt_broadcast)
        self.register_handler(Subscribe, self.__handle_add)

    def close_queue(self):
        self.__router_queue.close()

    def get_router_queue(self):
        return self.__router_queue

    def get_num_actors(self):
        return len(self.actor_set)

    def set_actor_system(self, actor_system, path):
        """
        Set the actor system for the router

        :param actor_system:  The actor system
        :type actor_system:  ActorSystem
        :param path:  The path to add the router to
        :type path:  str
        """
        self.actor_system = actor_system
        self.actor_system.add_actor(self, path)
        self.sys_path = "{}/{}".format(path, self.name)

        for actor in self.actor_set:
            self.actor_system.add_actor(actor, self.sys_path)

    async def __handle_add(self, message):
        """
        Handle actor addition

        :param message: The addition message
        :type message: Subscribe()
        """
        try:
            if message:
                actor = message.payload
                if actor and isinstance(actor, AbstractActor):
                    if actor not in self.actor_set:
                        self.actor_set.append(actor)
                else:
                    logging.warn("Router Only Accepts Abstract Actors")
        except Exception:
            self.handle_fail()

    def add_actor(self, actor):
        """
        Add an actor to the ready queue.

        :param actor:  A base actor for the router
        :type actor:  BaseActor
        """
        try:
            if actor.get_state() is ActorState.LIMBO:
                actor.start()
    
            if actor.get_state() is not ActorState.RUNNING:
                raise ActorStateError(
                    "Actor to Add to Round Robin Router Not Working")

            if actor not in self.actor_set:
                self.actor_set.append(actor)
                if self.address and actor.name:
                    node_addr = [x for x in self.address]
                    node_addr.append(actor.name)
                    registry.get_registry().add_actor(node_addr, actor, True)
            actor.__inbox = self.__router_queue
            if self.sys_path is not None and self.actor_system is not None:
                self.actor_system.add_actor(actor, self.sys_path)
        except Exception as e:
            self.handle_fail()

    def remove_actor(self, actor):
        """
        Remove an actor from the router.

        :param actor:  The implemented actor to remove
        :type actor:  BaseActor
        """
        try:
            if actor in self.actor_set:
                self.actor_set.remove(actor)
    
                # remove actor from system if set
                if self.sys_path is not None:
                    path = "{}/{}".format(self.sys_path, actor.get_name())
                    self.actor_system.delete_branch(path)
        except Exception as e:
            self.handle_fail()

    async def __handle_remove(self, message):
        """
        Remove an actor from the system.
        """
        try:
            if message:
                actor = message.payload
                if actor:
                    self.remove_actor(actor)
                else:
                    print("Actor to remove was None")
            else:
                print("Message to remove request empty")
        except Exception as e:
            self.handle_fail()

    async def route_ask(self, message):
        """
        Send an ask request to the queue.  The queue calls must block.
        """
        res = None
        try:
            assert isinstance(message.payload, QueryMessage)
            message = message.payload
            if not message.result:
                message.result = asyncio.Future(loop=self.loop)
            await self.__router_queue.put(message)
            res = await message.result
        except Exception as e:
            self.handle_fail()
        return res

    async def route_tell(self, message):
        """
        Send a tell request to the queue.  The queue calls on the actor
        should block.
        """
        try:
            await self.__router_queue.put(message.payload)
        except Exception as e:
            self.handle_fail()

    async def attempt_broadcast(self, message):
        """
        'Broadcast' a message to all actors in the router.  This currently
        pushes a message on the queue per actor so broadcast itself is not
        correct.
        """
        try:
            for actor in self.actors:
                await self.__router_queue._put(message)
        except Exception as e:
            self.handle_fail()
