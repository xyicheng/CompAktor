'''
Created on Sep 21, 2017

@author: aevans
'''

import asyncio
import random
from compaktor.actor.base_actor import BaseActor
from compaktor.errors.actor_errors import ActorStateError
from compaktor.message.message_objects import RouteAsk, RouteBroadcast,\
    RouteTell, DeSubscribe
from compaktor.registry import actor_registry as registry
from compaktor.state.actor_state import ActorState
from compaktor.utils.name_utils import NameCreationUtils


class RandomRouter(BaseActor):
    """
    Makes a random choice from the pool of actors and routes to the actor.
    """

    def __init__(self,  name=None, loop=None, address=None, mailbox_size=10000,\
                actors=[], inbox=None):
        if name is None:
            name = NameCreationUtils.get_name_base()
            name += "_"
            name += str(int(random.random() * 1000))
        if address is None:
            address = name
        super().__init__(name, loop, address, mailbox_size, inbox)
        self.actor_set = actors
        if self.actor_set:
            self.actor_set = list(set(self.actor_set))
        self.register_handler(RouteAsk, self.route_ask)
        self.register_handler(RouteTell, self.route_tell)
        self.register_handler(RouteBroadcast, self.broadcast)
        self.register_handler(DeSubscribe, self.__handle_remove)
        self.last_active_check = 0
        self.actor_system = None
        self.sys_path = None

    def get_num_actors(self):
        """
        Get the current number of actors
        """
        return len(self.actor_set)

    def set_actor_system(self, actor_system, path):
        """
        Set the actor system for the router

        :param actor_system:  The actor system
        :type actor_system:  ActorSystem
        :param path:  The path to add the router to
        :type path:  str
        """
        try:
            self.actor_system = actor_system
            self.actor_system.add_actor(self, path)
            self.sys_path = "{}/{}".format(path, self.name)
    
            for actor in self.actor_set:
                self.actor_system.add_actor(actor, self.sys_path)
        except Exception as e:
            self.handle_fail()

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
                    "Actor to Add to Random Router Not Working")
    
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

    def get_name(self):
        """
        Get the router name
        """
        return self.name

    def remove_actor(self, actor):
        """
        Remove an actor from the routers list of actors

        :param actor:  The actor to remove
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

    async def route_tell(self, message):
        """
        Submit a tell request to an actor from the specified sender.  On the
        100th call route_tell or route_ask the active queue is cleaned of
        dead actors.

        :param message:  The message to send
        :type message:  bytearray
        """
        try:
            actor = random.choice(self.actor_set)
    
            sender = message.sender
            if sender is None:
                sender = self
            fut = asyncio.run_coroutine_threadsafe(sender.tell(actor, message))
            fut.result(15)
        except Exception as e:
            self.handle_fail()

    async def route_ask(self, message):
        """
        Send an ask request to an actor in the router.

        :param message:  The message to send
        :type message:  bytearray
        """
        res = None
        try:
            actor = random.choice(self.actor_set)
            sender = message.sender
            if sender is None:
                sender = self
            res = await sender.ask(actor, message)
        except Exception as e:
            self.handle_fail()
        return res

    async def broadcast(self, message):
        """
        Broadcast a message to every actor in the router

        :param message:   Data message to send
        :type message:  The message
        """
        try:
            sender = self
            if message.sender is not None:
                sender = message.sender
    
            self.last_active_check += 1
            if self.last_active_check is 100:
                for actor in self.active_queue:
                    if actor.get_state() is not ActorState.RUNNING:
                        self.active_queue.remove(actor)
                self.last_active_check = 0
    
            for actor in self.actor_set:
                fut = asyncio.run_coroutine_threadsafe(
                    sender.tell(actor, message), loop=self.loop)
                fut.result(15)
        except Exception as e:
            self.handle_fail()
