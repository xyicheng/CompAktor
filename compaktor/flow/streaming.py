'''
Created on Aug 30, 2017

@author: aevans
'''


import asyncio
from enum import Enum
import logging
import pickle
import time


from compaktor.actor.actor import BaseActor
from compaktor.connectors.pub_sub import PubSub
from compaktor.actor.message import Message
from compaktor.gc.GCActor import GCActor, GCRequest


class MustBeSourceException(Exception): pass


class WrongActorException(Exception): pass


class SourceFunctionMissing(Exception): pass


class SourceMissing(Exception): pass


class Demand(Message): pass


class SetTickTime(Message): pass 

class Work(Message):
    """
    A message sent on receipt of demand or to a pub sub for flow processing.
    """
    
    
    def __init__(self, work_unit, sender = None):
        super().__init__(work_unit, sender)


class Pull(Message):
    """
    Submit a pull request containg the state
    """
    
    
    def __init__(self, state, sender = None):
        super().__init__(state, sender)


class Subscribe(Message):
    """
    A message used for subscription
    """
    
    def __init__(self, actor, sender = None):
        super().__init__(payload = actor, sender = sender)


class Tick(Message): pass


class DemandState(Enum):
    """
    Demand states for the actors
    """
    ACTIVE = 1
    BLOCKED = 2


class AccountActor(BaseActor):
    """
    Demand actor that accounts for finished work and pushes pull rates 
    to the source.  
    """
    
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._actor_map = {}
        self._source = kwargs.get('source', None)
        if self._source is not None and  isinstance(self._source, Source) is False:
            raise MustBeSourceException(
                    "Actor Provided as Source is not an Instance of Source!"
                )
        self._send_heartbeat = kwargs.get('max_wait',15) #in seconds
        self._last_send = time.time()
        self.register_handler(Demand, self.handle_demand)
        
    
    def handle_demand(self, message):
        kv = message.payload
        if kv is not None and 'key' in kv.keys():
            k = kv['key']
            v = kv['time']
            
            if k not in self.actor_map.keys():
                self._actor_map[k] = [v]
            elif len(self._actor_map[k]) < 3:
                self._actor_map[k].append(v)
            else:
                self._actor_map[k] = self._actor_map[k][-2:].append(v)
        
        if time.time() - self._last_send  >  self._send_heartbeat:
            #send off the average 
            self.tell(self._source, SetTickTime())
            self._last_send = time.time()
        

class TickActor(BaseActor):
    """
    The tick actor calls the tick method after a wait time.
    Back pressure uses time to slow down the rate of pull.
    """
    
    def __init__(self, *args, **kwargs):
        """
        Constructor
        
        :Keyword Arguments:
            *tick_time (double):  Time between pulls
            *source (Source):  Source to send pull request to
        """
        super().__init__(*args, **kwargs)
        self._tick_time = kwargs.get('tick_time', .25) #seconds
        self.register_handler(Tick, self.tick)
        self._source = kwargs.get('source', None)
        if self._source is None:
            raise SourceMissing("Source Must Be Provided for Tick Actor")
        self.loop.call_later(self._tick_time, Tick())
        
        
    def tick(self, message):
        """
        Perform action within each tick.
        
        :param message:  Calling message (not handled)
        :type message:  Tick
        """
        async def do_tick():
            self.tell(self._source, Pull())
        self.loop.run_until_complete(do_tick())
        self.loop.call_later(self._tick_time, self.tell(self, Tick()))
        
    
    def set_tick_time(self, message):
        """
        Set the time between ticks.
        
        :param message:  The message containing the tick time
        :type message:  SetTickTime
        """
        t = message.payload
        if t is not None and isinstance(t, float) or isinstance(t, int):
            self._tick_time = t
        
    
class Source(BaseActor):
    """
    The source actor.
    """
    
    def __init__(self, *args, **kwargs):
        """
        Constructor
        
        :Keyword Arguments:
            *gc_heart_beat (int): Optional number of seconds between heartbeats
            *on_pull (function): Required on pull function
        """
        super().__init__(*args, **kwargs)
        self._gc_heartbeat = kwargs.get('gc_heart_beat', 300) #seconds
        self._publisher = kwargs.get('pub_sub',PubSub())
        
        if isinstance(self._publisher, PubSub) is False:
            raise WrongActorException("pub_sub must be an instance of PubSub")
        
        self._on_pull = kwargs.get('on_pull', None)
        
        if self._on_pull is None:
            raise SourceFunctionMissing("Function on_pull is missing.")
        
        self._last_gc = time.time()
        self._gc_actor = GCActor()
        
        #create tick actor
        self._tick_actor = TickActor()
        self.loop.run_until_complete(self._tick_actor.start())
        
    
    def handle_pull(self, message):
        """
        Calls the pull function after receiving a request from the TickActor
        """
        
        result = self._on_pull()
        
        current_time = time.time()
        if self._last_gc - current_time > self._gc_heartbeat:
            async def call_gc_actor():
                await self.tell(self._gc_actor, GCRequest())
            self.loop.run_until_complete(call_gc_actor()) 
            self._last_gc = time.time()
        
    
class Sink(BaseActor):
    """
    The sink actor
    """
    
    def __init__(self):
        """
        Constructor
        
        
        """
        pass
    
    
    def handle_push(self, message):
        pass
