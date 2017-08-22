'''
A set of routers.

Created on Aug 19, 2017

@author: aevans
'''

import itertools
import logging
import sys
from atomos import atomic
from compaktor.actor.actor import BaseActor, ActorState
from compaktor.actor.message import Message


class RouteTell(Message): pass
    

class RouteAsk(Message): pass


class RouteBroadcast(Message): pass


class BalancingRouter(BaseActor):
    """
    The balancing pool router.  Routers do not use handlers. 
    """
    
    ready_queue = []
    active_queue = []
    
    
    def __init__(self, actors = [], *args, **kwargs):
        super.__init__(*args, **kwargs)
        self.last_active_check = 0
        self.ready_queue = list(set(actors))
        self.register_handler(RouteAsk, self.route_ask)
        self.register_handler(RouteTell, self.route_tell)
        self.register_hanlder(RouteBroadcast, self.broadcast)
        
        
    def add_actor(self, actor):
        """
        Add an actor to the ready queue.
        
        :param actor:  The actor to add to the queue
        """
        if actor not in self.ready_queue and actor not in self.active_queue:
            self.ready_queue.append(actor)
    
    
    def remove_actor(self,actor):
        """
        Remove an actor from the router.
        
        :param actor:  The implemented actor to remove
        :type actor:  BaseActor
        """
        if actor in self.ready_queue:
            self.ready_queue.remove(actor)
        elif actor in self.active_queue:
            self.active_queue.remove(actor)


    async def route_tell(self, message):
        """
        Submit a tell request to an actor from the specified sender.  On the
        100th call route_tell or route_ask the active queue is cleaned of 
        dead actors.
        
        :param message:  The message to send
        :type message:  bytearray
        """
        sender = self
        actor = None
        while len(self.ready_queue) > 0 and actor is None:
            #get an actor that is ready for work
            actor = self.ready_queue.pop(0)
            if actor.get_state() is not ActorState.RUNNING:
                actor = None
        
        self.last_active_check += 1
        if self.last_active_check is 100:
            for actor in self.active_queue:
                if actor.get_state() is not ActorState.RUNNING:
                    self.active_queue.remove(actor)
            self.last_active_check = 0

        sender = None        
        if message.sender is not None:
            sender = message.sender
        else:
            sender = actor
        
        if actor is not None:
            actor.tell(sender, message)
        
    
    async def route_ask(self, message):
        """
        Send an ask request to an actor in the router.
        
        :param message:  The message to send
        :type message:  bytearray
        """
        sender = self
        actor = None
        while len(self.ready_queue) > 0 and actor is None:
            #get an actor that is ready for work
            actor = self.ready_queue.pop(0)
            if actor.get_state() is not ActorState.RUNNING:
                actor = None
        
        self.last_active_check += 1
        if self.last_active_check is 100:
            for actor in self.active_queue:
                if actor.get_state() is not ActorState.RUNNING:
                    self.active_queue.remove(actor)
            self.last_active_check = 0

        sender = None        
        if message.sender is not None:
            sender = message.sender
        else:
            sender = actor
        
        if actor is not None:
            actor.ask(sender, message)
    
    
    async def broadcast(self, message):
        """
        Broadcast a message to every actor in the router
        
        :param message:   Data message to send 
        :type message:  The message
        """
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
            sender.tell(actor, message)


class RoundRobinRouter(BaseActor):
    """
    A round robin router actor that facilitates messaging serially between
    a set of actors.  Routers do not use handlers.
    """
    actor_set = []
    current_index = atomic.AtomicInteger()
    
    
    def __init__(self, actors = [], *args, **kwargs):
        super.__init__(*args, **kwargs)
        self.actor_set = list(set(actors))
    
    
    def add_actor(self, actor):
        """
        Add an actor to the router's SET of actors.
        
        :param actor:  The actor to add to the router
        """
        if actor not in self.actors:
            self.actor_set.append(actor)
    
    
    def remove_actor(self,actor):
        """
        Remove an actor from the routers list of actors
        
        :param actor:  The actor to remove
        """
        if actor in self.actor_set:
            self.actor_set.remove(actor)
    

    async def route_tell(self, message):
        """
        Submit a tell request to an actor from the specified sender.
        
        :param message:  The message to send
        :type message:  bytearray
        """
        sender = self
        if message.sender is not None:
            sender = message.sender
            
        ind = self.current_index.get()
        sender.tell(self.actor_set[ind],message)
        self.current_index.get_and_add(1)
        if self.current_index.get() is len(self.actor_set):
            self.current_index.get_and_set(0)
        
        
    
    async def route_ask(self, message):
        """
        Send an ask request to an actor in the router.
        
        :param message:  The message to send
        :type message:  bytearray
        """
        sender = self
        if message.sender is not None:
            sender = message.sender
        sender.ask(self.actor_set[self.current_index],message)
        self.current_index.get_and_add(1)
        if self.current_index.get() is len(self.actor_set):
            self.current_index.get_and_set(0)
    
    
    async def broadcast(self, message):
        """
        Broadcast a message to every actor in the router
        
        :param message:   Data message to send 
        :type message:  The message
        """
        sender = self
        if message.sender is not None:
            sender = message.sender
            
        for actor in self.actor_set:
            sender.tell(actor, message)
