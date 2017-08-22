'''
A set of routers.

Created on Aug 19, 2017

@author: aevans
'''

import itertools
import sys
from atomos import atomic
from compaktor.actor.actor import BaseActor
from compaktor.actor.message import Message


class RouteTell(Message): pass
    

class RouteAsk(Message): pass


class BalancingRouter(BaseActor):
    """
    The balancing pool router.  Routers do not use handlers. 
    """
    
    ready_queue = []
    active_queue = []
    
    
    def __init__(self, actors = [], *args, **kwargs):
        self.message_timeout = 10
        super.__init__(*args, **kwargs)
        self.ready_queue = list(set(actors))
        self.register_handler(RouteAsk, self.route_ask)
        self.register_handler(RouteTell, self.route_tell)
        
        
    def add_actor(self, actor):
        """
        Add an actor to the  
        """
        pass
    
    
    def remove_actor(self,actor):
        """
        Remove an actor from the router.
        
        :param actor:  The implemented actor to remove
        :type actor:  BaseActor
        """
        pass


    async def route_tell(self, message, sender = self, timeout = 10):
        """
        Tell an actor in the ready queue.
        
        :param message:  The message to send
        :type message:  bytearray
        :param sender:  The implemented actor routing the message
        :type sender:  BaseActor
        :param timeout:  The timeout to wait for in seconds
        :type timeout:  int
        """
        pass
    
    
    async def route_ask(self, message, sender = self, timeout = 10):
        """
        Send and ask request. 
        
        :param message:  The message to send
        :type message:  bytearray
        :param sender:  The implemented sending actor
        :type sender:  BaseActor
        :param timeout:  The timeout to wait before failing
        :type timeout:  int
        """
        pass
    
    
    async def broadcast(self):
        pass


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
    

    def route_tell(self, message, sender = self):
        """
        Submit a tell request to an actor from the specified sender.
        
        :param message:  The message to send
        :type message:  bytearray
        :param sender:  The sender impmented by the user
        :type sender:  BaseActor
        """
        ind = self.current_index.get()
        sender.tell(self.actor_set[ind],message)
        self.current_index.get_and_add(1)
        if self.current_index.get() is len(self.actor_set):
            self.current_index.get_and_set(0)
        
        
    
    def route_ask(self, message, sender = self):
        """
        Send an ask request to an actor in the router.
        
        :param message:  The message to send
        :type message:  bytearray
        :param sender:  The sender actor implemented from an actor class
        :type sender:   BaseActor
        """
        sender.ask(self.actor_set[self.current_index],message)
        self.current_index.get_and_add(1)
        if self.current_index.get() is len(self.actor_set):
            self.current_index.get_and_set(0)
    
    
    async def broadcast(self, message, sender = self):
        """
        Broadcast a message to every actor in the router
        
        :param sender:   The user implemented sender actor 
        :type sender: BaseActor
        """
        for actor in self.actor_set:
            sender.tell(actor, message)
