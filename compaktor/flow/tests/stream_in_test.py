'''
Created on Sep 4, 2017

@author: aevans
'''

import asyncio
import datetime
import math
import time
from atomos.atomic import AtomicFloat
from compaktor.actor.actor import BaseActor, ActorState
from compaktor.flow.streaming import Tick, Pull, SetTickTime, AccountingActor,\
    Sink, Subscribe
from asyncio.events import AbstractEventLoop
from twisted.python.randbytes import SourceNotAvailable
from compaktor.actor.message import QueryMessage, Message, PoisonPill
from threading import Thread
from concurrent.futures.thread import ThreadPoolExecutor


class Pull(Message): pass


class SourceNotProvided(Exception): pass


class KwargTypeIncorrect(Exception): pass


class ArgTypeIncorrect(Exception): pass


class TickActor(BaseActor):
    """
    The TickActor is used to signal the source.  The actor is the go between
    with the source.  The Flow, TickActor, Demand, and source must be on the
    same machine.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._source = kwargs.get('source', None)
        if self._source is None:
            SourceNotProvided("Source Not Provided to Tick Actor.")        

        self._tick_time = kwargs.get('tick_time', float(10))
        if not type(self._tick_time) in [int, float]:
            raise KwargTypeIncorrect("Tick Time Must be int or float")
        self._tick_time = AtomicFloat(float(self._tick_time))
        self.register_handler(SetTickTime, self._set_tick_time)
        
    def get_tick_time(self):
        """
        Returns the tick time
        
        :return: The tick time
        :rtype: float
        """
        return self._tick_time.get()

    async def _tick(self):
        """
        The internal function actually performing a tick.
        """
        await self.tell(self._source, Pull())
    
    def tick(self):
        """
        Call and wait for completion of a _tick
        """
        self.loop.run_until_complete(self._tick())
    
    def _set_tick_time(self, message):
        """
        Set our ticktime.
        
        :param message: Message triggering the setter
        :type message: SetTickTime()
        """
        new_tick_time = message.payload
        if not type(new_tick_time) in [int, float]:
            raise ValueError("Tick Time to Set not Float or Int.")
        self._tick_time = new_tick_time
    
    async def _do_terminate(self):
        """
        Runs the termination component
        """
        await self.tell(self._source, PoisonPill())
    
    def terminate(self):
        """
        Send a Poison Pill up the actor flow chain
        """
        self.loop.run_until_complete(self._do_terminate())
    

class FlowRunner(object):
    """
    This is required to kick off the flow.  All actors should be started first
    before being connected to the flow. FlowRunner is closeable, hence the 
    verb. 
    """
    
    def __init__(self):
        self._futures = []
        self._executor = ThreadPoolExecutor()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
    
    async def _close_actor(self, actor):
        if isinstance(actor, BaseActor):
            if actor.get_state() is ActorState.RUNNING:
                await actor.tell(actor, PoisonPill())    
    
    def close(self):
        """
        Shutdown the executor pool
        """
        self._executor.shutdown(120)
    
    def create_new_flow(self, source, accounting_actor=None):
        """
        Create a new flow which is added to the futures list and managed by the 
        executor when run is called.
        """
        if accounting_actor is not None and isinstance(accounting_actor, 
                                                  AccountingActor) is False:
            raise ArgTypeIncorrect(
                "Demand Actor must be instance of Accounting Actor")
        
        if source and isinstance(source, BaseActor) is False:
            raise SourceNotAvailable("Must Provide Source to Flow.")
        self._source = source
        kwargs = {'source' : self._source}
        self._tick_actor = TickActor(*[], **kwargs)
        self._accounting_actor = accounting_actor
        self._current_actors = [source]
        self._accounting_actor.start()
        self._source.start()
    
    async def _subscribe(self, stage_from, stage_to):
        """
        The stage_to subscribes to the stage_from.
        """
        if isinstance(stage_from, BaseActor) and isinstance(stage_to, 
                                                            BaseActor):
            raise ArgTypeIncorrect(
                "Both subscription arguments must be actors.")
        await stage_to.tell(stage_from, Subscribe(stage_to)) 
    
    async def _do_subscribe(self, pub, sub):
        """
        Perform a subscription to a publisher
        
        :param pub:  The publisher actor
        :type pub: PubSub()
        :param sub:  The subscription actor
        :type sub:  BaseActor
        """
        if pub and sub:
            await sub.tell(pub, Subscribe(sub))
    
    def to(self, sink):
        """
        Connect a Sink in the final step of the flow.
        
        :param sink:  The Sink
        :type sink:  Sink
        """
        if isinstance(sink, Sink) is False:
            raise ArgTypeIncorrect("To Must be Provided a Sink")
        for actor in self._current_actors:
            asyncio.get_event_loop().run_until_complete(self._do_subscribe(
                                                        actor, sink))
        
    def merge(self, actor):
        """
        Merge all actors in the current actors set together using 
        the provided stage.
        
        :param actor:  The actor for merging
        :type actor: Stage()
        """
        pass
    
    def connect_chain(self, builder):
        """
        Connect a chain from the chain builder.
        
        :param builder:  A FlowChainBuilder
        :type builder: FlowChainBuilder()
        """
        pass
    
    def map(self, func, map_idx = None):
        """
        Builds a map actor with the function from the specifid indices.  These
        indices map to the current_actors.  If map_idx is None, all actors are
        attached to.
        
        :param func:  The mapping function
        :type func: def
        :param map_idx:  The current actor indices
        :type map_idx: list()
        """
        pass
    
    def run(self):
        """
        Run a stream to termination. This starts a loop which pushes to the tick
        actor. Your main thread is unaffected but a new thread runs the flow.
        
        :return: A thread containing the running loop
        :rtype: Thread()
        """
        pass  


class FlowChainBuilder:
    """
    A builder for a secondary stream of actors.
    """
    def __init__(self):
        pass

if __name__ == "__main__":
    tick_actor = TickActor()
