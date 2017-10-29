'''
Created on Sep 21, 2017

@author: aevans
'''

import asyncio
from atomos import atomic
from compaktor.actor.base_actor import BaseActor
from compaktor.errors.actor_errors import ActorStateError
from compaktor.message.message_objects import RouteAsk, RouteTell, DeSubscribe,\
    Subscribe
from compaktor.registry import actor_registry as registry
from compaktor.state.actor_state import ActorState
from compaktor.utils.name_utils import NameCreationUtils
from random import random
from compaktor.actor.abstract_actor import AbstractActor
import logging
import pdb

class RoundRobinRouter(BaseActor):
    """
    A round robin router actor that facilitates messaging serially between
    a set of actors.  Routers do not use handlers.
    """

    def __init__(self, name=None, loop=None, address=None, mailbox_size=10000,
                 inbox=None, actors=[]):
        if name is None:
            name = NameCreationUtils.get_name_base()
            name = NameCreationUtils.get_name_and_number(str(name))
        if address is None:
            address = name
        super().__init__(name, loop, address, mailbox_size, inbox)
        self.name = name
        self.actor_set = actors
        self.current_index = atomic.AtomicInteger()
        self.actor_system = None
        self.sys_path = None
        self.set_router_handlers()

    def set_router_handlers(self):
        """
        Register the actor handlers
        """
        self.register_handler(RouteTell, self.route_tell)
        self.register_handler(RouteAsk, self.route_ask)
        self.register_handler(DeSubscribe, self.__handle_remove)
        self.register_handler(Subscribe, self.__handle_add)

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

    def add_actor(self, actor):
        """
        Add an actor to the router's SET of actors.

        :param actor:  The actor to add to the router
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
                    registry.get_registry().add_actor(node_addr, actor, True)
                    actor.set_address(node_addr)

            if self.sys_path is not None and self.actor_system is not None:
                self.actor_system.add_actor(actor, self.sys_path)
        except Exception as e:
            self.handle_fail()

    def get_num_actors(self):
        return len(self.actor_set)

    def remove_actor(self, actor):
        """
        Remove an actor from the routers list of actors

        :param actor:  The actor to remove
        """
        if actor in self.actor_set:
            self.actor_set.remove(actor)

        # remove actor from system if set
        if self.sys_path is not None:
            path = "{}/{}".format(self.sys_path, actor.get_name())
            self.actor_system.delete_branch(path)

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

    async def route_tell(self, message):
        """
        Submit a tell request to an actor from the specified sender.

        :param message:  The message to send
        :type message:  bytearray
        """
        try:
            sender = self
            if message.sender is not None:
                sender = message.sender
            ind = self.current_index.get()
            if self.actor_set and len(self.actor_set) > 0:
                actor = self.actor_set[ind % len(self.actor_set)]
                if self.loop == actor.loop:
                    await self.tell(actor, message.payload)
                else:
                    asyncio.run_coroutine_threadsafe(
                        self.tell(actor, message.payload), loop=actor.loop)
                self.current_index.get_and_add(1)
            if self.current_index.get() is len(self.actor_set):
                self.current_index.get_and_set(0)
        except Exception as e:
            self.handle_fail()

    def get_current_index(self):
        """
        The current index for the router.  Useful for debugging.

        :return:  The current index
        :rtype: int()
        """
        return self.current_index.get()

    async def route_ask(self, message):
        """
        Send an ask request to an actor in the router.

        :param message:  The message to send
        :type message:  bytearray
        :return: The result from the ask function
        :rtype: object
        """
        sender = self
        res = None
        if len(self.actor_set) > 0:
            if message.sender is not None:
                sender = message.sender
            ind = self.current_index.get()
            message = message.payload
            await sender.ask(self.actor_set[ind], message)
            if isinstance(message, RouteAsk):
                message = message.payload
            res = await message.result
            self.current_index.get_and_add(1)
            if self.current_index.get() is len(self.actor_set):
                self.current_index.get_and_set(0)
        else:
            msg = "Actor Set Size is 0 in router."
            msg += "\n{}\n{}".format(self, message.sender)
            logging.error(msg)
        return res

    async def broadcast(self, message):
        """
        Broadcast a message to every actor in the router

        :param message:   Data message to send
        :type message:  The message
        """
        sender = self
        if message.sender is not None:
            sender = message.sender

        rfuncs = []
        for actor in self.actor_set:
            fut = asyncio.run_coroutine_threadsafe(
                sender.tell(actor, message), self.loop)
            rfuncs.append(fut)
        for func in rfuncs:
            func.result(15)
