'''
Created on Aug 30, 2017

@author: aevans
'''


import asyncio
from enum import Enum
import logging
import math
import pickle
import time
import traceback
from compaktor.actor.actor import BaseActor
from compaktor.connectors.pub_sub import PubSub
from compaktor.actor.message import Message
from compaktor.gc.GCActor import GCActor, GCRequest


class MustBeSourceException(Exception): pass


class WrongActorException(Exception): pass


class SinkFunctionMissing(Exception): pass


class SourceFunctionMissing(Exception): pass


class AccountingActorNotSuppliedException(Exception): pass


class SourceMissing(Exception): pass


class Demand(Message): pass


class SetTickTime(Message): pass 


class FlowResult(Message): pass 


class Push(Message): pass


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


class AccountingActor(BaseActor):
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
                current_avg = 0 
                for k in self._actor_map:
                    d = self._actor_map[k]
                    if len(d) > 0:
                        avg = sum(d) / len(d)
                        if avg  > current_avg:
                            current_avg = avg
                self.tell(self._source, SetTickTime(current_avg))
                self._last_send = time.time()
        else:
            logging.warn(
                "Message Must be of Type Demand. Received {}".format(type(message))
                )
        

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
        while True:
            print("Tick")
            async def do_tick():
                await self.tell(self._source, Pull())
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
        
        self._on_pull = kwargs.get('pull_function', None)
        
        if self._on_pull is None:
            raise SourceFunctionMissing("Function on_pull is missing.")
        
        self._last_gc = time.time()
        self._gc_actor = GCActor()
        
        #create tick actor
        tick_args = {'source' : self}
        self._tick_actor = TickActor(*[], **tick_args)
        self._tick_actor.start()
        self.children = [self._tick_actor]
        self.register_handler(Push, self.handle_pull)
        self.register_handler(Subscribe, self._do_subscribe)

    def subscribe(self, actor):
        """
        Subscribe to the pubsub on the source (connect an output)
        """
        if isinstance(actor, BaseActor) is False:
            raise ValueError("Subscriber to the Source must be  a Base Actor.")
        self._publisher.subscribe(actor)
    
    def _do_subscribe(self, actor):
        """
        Perform the Subscribe from an actor message
        """
        self.subscribe(actor.payload)
    
    def handle_pull(self, message):
        """
        Calls the pull function after receiving a request from the TickActor
        """
        
        result = self._on_pull()
        self.tell(self._publisher, FlowResult(result))
        current_time = time.time()
        if self._last_gc - current_time > self._gc_heartbeat:
            async def call_gc_actor():
                await self.tell(self._gc_actor, GCRequest())
            self.loop.run_until_complete(call_gc_actor()) 
            self._last_gc = time.time()
        

class Sink(BaseActor):
    
    
    def __init__(self, *args, **kwargs):
        """
        Constructor
        
        :Keyword Arguments:
            *accounting_calc_heartbeat (double): Time between accounting actions
            *accounting_actor (AccountingActor): Actor for accounting
            *push_function (def): Function performed on push
        """
        super().__init__(*args, **kwargs)
        self._accounting_calc_heartbeat = kwargs.get('accounting_calc_heartbeat', 30)
        self._accounting_actor = kwargs.get('accounting_actor', None)
        self._on_push = kwargs.get('push_function', None)
        
        if self._on_push is None:
            raise SinkFunctionMissing("Sink Function must be supplied")
        
        if self._accounting_actor is None:
            raise AccountingActorNotSuppliedException(
                "Accounting actor not Supplied for Sink"
                )
        elif isinstance(self._accounting_actor, AccountingActor) is False:
            raise AccountingActorNotSuppliedException(
                    "Accounting Actor for Sink Cannot be of Type {}"
                    .format(type(AccountingActor))
                )
        
        self._last_demand = time.time()
        self.register_handler(FlowResult, self.handle_push)

    def handle_push(self, message):
        push_time = time.time()
        
        self.tell(self._publisher,message)
        
        if time.time() - self._last_accounting > self._accounting_calc_heartbeat:
            push_time = time.time() - push_time
            self.tell(self._accounting_actor, Demand(push_time))
            self._last_accounting = time.time()
    
    
class PubSubSink(BaseActor):
    """
    The sink actor
    """
    
    def __init__(self, *args, **kwargs):
        """
        Constructor
        
        :Keyword Arguments:
            *accounting_calc_heartbeat (double):  Time between demand calculation
            *accounting_actor (AccountingActor): Actor for performing calculations
            *pub_sub (PubSub):  A PubSub actor to use
            *subscribers (list):  The list of subscribers. At least one initially.
        """
        super().__init__(*args, **kwargs)
        self._accounting_calc_heartbeat = kwargs.get('accounting_calc_heartbeat', 30)
        self._publisher = kwargs.get('pub_sub', PubSub())
        
        if self._push_function is None:
            raise SinkFunctionMissing(
                "push_function must be specified with a Sink"
                )
            
        self._subscriptions = kwargs.get('subscribers', None)
        if self._subscriptions is not None:
            if len(self._subscriptions) > 0:
                for subscriber in self._subscriptions:
                    if isinstance(subscriber, PubSub) is False:
                        raise WrongActorException("Subscriber in sink must be a PubSub")
                    
                    self.loop.run_until_complete(self.subscribe(subscriber)) #block on tell
                    
            else:
                raise ValueError("Subscribers Cannot be Empty for Sink")
        else:
            raise ValueError("Subscribers must be provided for Sink")
            
        
        self._accounting_actor = kwargs.get('accounting_actor', None)
        
        if self._accounting_actor is None or isinstance(self._accounting_actor, AccountingActor) is False:
            raise AccountingActorNotSuppliedException("Accounting actor not Supplied for Sink")
        
        self._last_demand = time.time()
        self.register_handler(FlowResult, self.handle_push)
    
    async def subscribe(self, subscriber):
        await self.tell(subscriber, Subscribe(self))
    
    def handle_push(self, message):
        push_time = time.time()
        
        self.tell(self._publisher,message)
        
        if time.time() - self._last_accounting > self._accounting_calc_heartbeat:
            push_time = time.time() - push_time
            self.tell(self._accounting_actor, Demand(push_time))
            self._last_accounting = time.time()


class DataFrameSink(BaseActor):
    """
    This sink takes in data and appends to a data frame using specified arguments.
    """
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class Stage(BaseActor):    
    
    def __init__(self, *args, **kwargs):
        """
        Constructor
        
        :Keyword Arguments:
            *accounting_calc_heartbeat (double):  Time between demand calculation
            *push_function (function):  The function to use when pushing
            *publisher (PubSub): The output publishers
            *subscriptions (list): A list of PubSubs to subscribe to 
        """
        super().__init__(*args, **kwargs)
        self._publisher = kwargs.get('publisher', PubSub())
        
        if self._publisher is None or isinstance(self._publisher, PubSub) is False:
            raise WrongActorException(
                "Publisher Must be an instance of PubSub in Stage"
                )
        self._accounting_calc_heartbeat = kwargs.get('accounting_calc_heartbeat', 30)
        self._push_function = kwargs.get('push_function', None)
        
        if self._push_function is None or isinstance(self._push_function, PubSub) is False:
            raise SinkFunctionMissing(
                "push_function must be specified with a Sink"
                )
        
        self._accounting_actor = kwargs.get('demand_actor', None)
        
        if self._accounting_actor is None or isinstance(self._accounting_actor, AccountingActor) is False:
            raise AccountingActorNotSuppliedException("Accounting actor not Supplied for Sink")
        
        self._last_demand = time.time()
        self.register_handler(FlowResult, self.handle_push)
    
    def subscribe(self, message):
        pass
    
    def _do_subscribe(self, message):
        """
        Perform the Subscribe from an actor message
        """
        self.subscribe(message.payload)
    
    def handle_push(self, message):
        push_time = time.time()
        
        result = self._push_function(message)
        self.tell(self._publisher, FlowResult(result))
        
        if time.time() - self._last_accounting > self._accounting_calc_heartbeat:
            push_time = time.time() - push_time
            self.tell(self._accounting_actor, Demand(push_time))
            self._last_accounting = time.time()
            

class FlowControls():
    
    def __init__(self):
        pass
    
    def manage_stream(self,source):
        pass
