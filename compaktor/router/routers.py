'''
A set of routers.

Created on Aug 19, 2017

@author: aevans
'''


import asyncio
import random
from atomos import atomic
import janus
from compaktor.actor.actor import BaseActor, ActorState, QueryMessage
from compaktor.actor.message import Message


class RouteTell(Message): pass
    

class RouteAsk(Message): pass


class RouteBroadcast(Message): pass


class BalancingRouter(BaseActor): 
    """
    The balancing router uses a single message queue to route messages to 
    actors.  Every actor shares the common queue.  This requires reworking
    the actors to allow a special blocking queue.
    """
    
    
    def __init__(self, actors = [], *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.actors = actors
        self.max_inbox_size = kwargs.get('max_inbox_size',0)
        self._queue = janus.Queue(maxsize = self.max_inbox_size,
                                    loop = self.loop)
        raise Exception("Not Implemented Yet")
    
    
    def create_actor(self, actor_ref):
        """
        Add an actor to the ready queue.
        
        :param actor:  The actor ref to instantiatable code implementing Base Actor
        :type actor:  reference
        """
        args = []
        kwargs = {"loop" : self.loop, "queue" : self._queue}
        actor = actor_ref(*args)
        if actor not in self.actors:
            self.actors.append(actor)
    
    
    def remove_actor(self,actor):
        """
        Remove an actor from the router.
        
        :param actor:  The implemented actor to remove
        :type actor:  BaseActor
        """
        if actor not in self.actors:
            self.actors.remove(actor)
    
    
    async def route_ask(self, message):
        """
        Send an ask request to the queue.  The queue calls must block. 
        """
        assert isinstance(message, QueryMessage)
        if not message.result:
            message.result = asyncio.Future(loop = self.loop)
        await self._queue.put(message)
        res = await message.result
        return res 
    
    
    async def route_tell(self, message):
        """
        Send a tell request to the queue.  The queue calls on the actor should block.
        """
        self._queue._put(message)
        
    
    async def attempt_broadcast(self, message):
        """
        'Broadcast' a message to all actors in the router.  At the moment this just pushes
        a message on the queue per actor so broadcast itself is not correct.
        """ 
        for actor in self.actors:
            self._queue._put(message)
    
    
class RandomRouter(BaseActor): 
    """
    Makes a random choice from the pool of actors and routes to the actor.
    """
    
    
    def __init__(self, actors = [], *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.actors = list(set(actors))
        self.register_handler(RouteAsk, self.route_ask)
        self.register_handler(RouteTell, self.route_tell)
        self.register_hanlder(RouteBroadcast, self.broadcast)
        
        
    def add_actor(self, actor):
        """
        Add an actor to the ready queue.
        
        :param actor:  The actor to add to the queue
        """
        if actor not in self.actors:
            self.actors.append(actor)
    
    
    def remove_actor(self,actor):
        """
        Remove an actor from the router.
        
        :param actor:  The implemented actor to remove
        :type actor:  BaseActor
        """
        if actor not in self.actors:
            self.actors.remove(actor)


    async def route_tell(self, message):
        """
        Submit a tell request to an actor from the specified sender.  On the
        100th call route_tell or route_ask the active queue is cleaned of 
        dead actors.
        
        :param message:  The message to send
        :type message:  bytearray
        """
        actor = random.choice(self.actors)
        
        sender = message.sender        
        if sender is None:
            sender = self
        
        sender.tell(actor, message)
            
    
    async def route_ask(self, message):
        """
        Send an ask request to an actor in the router.
        
        :param message:  The message to send
        :type message:  bytearray
        """
        actor = random.choice(self.actors)
        sender = message.sender
        if sender is None:
            sender = self
        sender.ask(sender, message)
        
    
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
    
    
    def __init__(self, actors = [], *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = kwargs.get('name',super().get_name())
        self.actor_set = list(set(actors))
        self.actor_set = []
        self.current_index = atomic.AtomicInteger()
        self.actor_system = None
        self.sys_path = None
    
    
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
        self.sys_path = "{}/{}".format(path,self.name)
        
        for actor in self.actor_set:
            self.actor_system.add_actor(actor, self.sys_path)
    
    
    def add_actor(self, actor):
        """
        Add an actor to the router's SET of actors.
        
        :param actor:  The actor to add to the router
        """
        if actor not in self.actor_set:
            self.actor_set.append(actor)
        
        if self.sys_path is not None and self.actor_system is not None:
            self.actor_system.add_actor(actor, self.sys_path)
            
    
    def get_num_actors(self):
        return len(self.actor_set)
    
    
    def remove_actor(self,actor):
        """
        Remove an actor from the routers list of actors
        
        :param actor:  The actor to remove
        """
        if actor in self.actor_set:
            self.actor_set.remove(actor)
            
        #remove actor from system if set
        if self.sys_path is not None:
            path = "{}/{}".format(self.sys_path,actor.get_name())
            self.actor_system.delete_branch(path)
    

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
        await sender.tell(self.actor_set[ind],message)
        self.current_index.get_and_add(1)
        if self.current_index.get() is len(self.actor_set):
            self.current_index.get_and_set(0)
        
    
    def get_current_index(self):
        """
        The current index for the router.  Useful for debugging.
        
        :return:  The current index
        """
        return self.current_index.get()
        
    
    async def route_ask(self, message):
        """
        Send an ask request to an actor in the router.
        
        :param message:  The message to send
        :type message:  bytearray
        """
        sender = self
        if message.sender is not None:
            sender = message.sender
        ind = self.current_index.get()
        res = await sender.ask(self.actor_set[ind],message)
        self.current_index.get_and_add(1)
        if self.current_index.get() is len(self.actor_set):
            self.current_index.get_and_set(0)
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
            rfuncs.append(sender.tell(actor, message))
        
        for func in rfuncs:
            await func
