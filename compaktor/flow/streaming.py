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
from compaktor.actor.actor import BaseActor, ActorState, ActorStateError
from compaktor.connectors.pub_sub import PubSub, Publish
from compaktor.actor.message import Message
from compaktor.gc.GCActor import GCActor, GCRequest
from atomos.atomic import AtomicFloat
from compaktor.utilities.type_utils import is_num
import functools

class MustBeSourceException(Exception):
    pass


class WrongActorException(Exception):
    pass


class SinkFunctionMissing(Exception):
    pass


class SourceFunctionMissing(Exception):
    pass


class AccountingActorNotSuppliedException(Exception):
    pass


class SourceMissing(Exception):
    pass


class Demand(Message):
    pass


class SetTickTime(Message):
    pass


class FlowResult(Message):
    pass


class Push(Message):
    pass


class Pull(Message):
    """
    Submit a pull request containg the state
    """

    def __init__(self, state, sender=None):
        super().__init__(state, sender)


class Subscribe(Message):
    """
    A message used for subscription
    """

    def __init__(self, actor, sender=None):
        super().__init__(payload=actor, sender=sender)


class Tick(Message):
    pass


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
        if self._source and isinstance(self._source, Source) is False:
            raise MustBeSourceException(
                "Actor Provided as Source is not an Instance of Source!"
            )
        self._send_heartbeat = kwargs.get('max_wait', 15)  # in seconds
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

            if time.time() - self._last_send > self._send_heartbeat:
                # send off the average
                current_avg = 0
                for k in self._actor_map:
                    d = self._actor_map[k]
                    if len(d) > 0:
                        avg = sum(d) / len(d)
                        if avg > current_avg:
                            current_avg = avg
                self.tell(self._source, SetTickTime(current_avg))
                self._last_send = time.time()
        else:
            logging.warn(
                "Message Must be of Type Demand. Received {}".format(
                    type(message))
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
        self._tick_time = kwargs.get('tick_time', .25)  # seconds

        if is_num(self._tick_time):
            self._tick_time = AtomicFloat(float(self._tick_time))
        else:
            raise TypeError("Tick Time must be a number")

        self._source = kwargs.get('source', None)
        if self._source is None:
            raise SourceMissing("Source Must Be Provided for Tick Actor")
        self._do_loop()

    async def tick(self, message):
        """
        Perform action within each tick.

        :param message:  Calling message (not handled)
        :type message:  Tick
        """
        await self.tell(self._source, Pull(self.get_state()))

    async def __handle_tick_iteration(self):
        await self.tick(Tick())
        self.loop.call_later(self._tick_time.get(),functools.partial(self._do_loop))

    def set_tick_time(self, message):
        """
        Set the time between ticks.

        :param message:  The message containing the tick time
        :type message:  SetTickTime
        """
        t = message.payload
        if t is not None and is_num(t):
            self._tick_time.set(float(t))
        else:
            logging.warn(
                "Can only Set Tick Time with a float or int\
                 ({})".format(str(t)))

    def _do_loop(self):
        """
        Starts the tick loop. Called by the constructor
        """
        asyncio.ensure_future(self.__handle_tick_iteration())


class Source(BaseActor):
    """
    The source actor.
    """

    def __init__(self, *args, **kwargs):
        """
        Constructor

        :Keyword Arguments:
            *gc_heart_beat (int): Optional seconds between heartbeats
            *on_pull (function): Required on pull function
        """
        super().__init__(*args, **kwargs)
        self._gc_heartbeat = kwargs.get('gc_heart_beat', 300)  # seconds
        self._publisher = kwargs.get('pub_sub', PubSub())
        if isinstance(self._publisher, PubSub) is False:
            raise WrongActorException("pub_sub must be an instance of PubSub")
        
        if self._publisher.get_state() is ActorState.LIMBO:
            self._publisher.start()
        
        if self._publisher.get_state() is not ActorState.RUNNING:
            raise ActorStateError(
                "Publisher is not Started and Not Startable in Source")
        
        self._on_pull = kwargs.get('pull_function', None)

        if self._on_pull is None:
            raise SourceFunctionMissing("Function on_pull is missing.")

        self._last_gc = time.time()
        self._gc_actor = GCActor()

        # create tick actor
        tick_args = {'source': self}
        self._tick_actor = TickActor(*[], **tick_args)
        self._tick_actor.start()
        self.children = [self._tick_actor]
        self.register_handler(Publish, self.__handle_pull)
        self.register_handler(Pull, self.__handle_pull)
        self.register_handler(Push, self.__handle_pull)
        self.register_handler(Subscribe, self.subscribe)

    def get_publisher(self):
        """
        Returns the publisher for more immediate subscription.
        :return: The publisher
        :rtype: PubSub()
        """
        return self._publisher

    async def __subscribe(self, message):
        """
        Subscribe to the pubsub on the source (connect an output)
        """
        try:
            actor = message.payload
            if isinstance(actor, BaseActor) is False:
                raise ValueError("Subscriber to the Source must be  a Base Actor.")
            self._publisher.subscribe(actor)
        except Exception as e:
            self.handle_fail()

    async def __handle_subscribe(self, actor):
        """
        Perform the Subscribe from an actor message
        """
        try:
            await self.__do_subscribe(actor.payload)
        except Exception as e:
            self.handle_fail()

    async def subscribe(self, message):
        try:
            await self.__subscribe(message)
        except Exception as e:
            self.handle_fail()

    async def __handle_pull(self, message):
        """
        Calls the pull function after receiving a request from the TickActor
        """
        try:
            result = self._on_pull(message)
            pub = Publish(FlowResult(result))
            await self.tell(self._publisher, pub)
            current_time = time.time()
            if self._last_gc - current_time > self._gc_heartbeat:
                async def call_gc_actor():
                    await self.tell(self._gc_actor, GCRequest())
                await call_gc_actor()
                self._last_gc = time.time()
        except Exception as e:
            self.handle_fail()

    def do_pull(self, message):
        self.loop.run_until_complete(self.handle_pull(message))


class Sink(BaseActor):

    def __init__(self, *args, **kwargs):
        """
        Constructor

        :Keyword Arguments:
            *accounting_calc_heartbeat (double): Time between actions
            *accounting_actor (AccountingActor): Actor for accounting
            *push_function (def): Function performed on push
        """
        super().__init__(*args, **kwargs)
        self._accounting_calc_heartbeat = kwargs.get(
            'accounting_calc_heartbeat', 30)
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

        self.tell(self._publisher, message)
        accounting = time.time() - self._last_accounting
        if accounting > self._accounting_calc_heartbeat:
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
            *accounting_calc_heartbeat (double):  Time between demand
            *accounting_actor (AccountingActor): Actor for accounting
            *pub_sub (PubSub):  A PubSub actor to use
            *subscribers (list):  The list of subscribers (one min)
        """
        super().__init__(*args, **kwargs)
        self._accounting_calc_heartbeat = kwargs.get(
            'accounting_calc_heartbeat', 30)
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
                        raise WrongActorException(
                            "Subscriber in sink must be a PubSub")

                    self.loop.run_until_complete(
                        self.subscribe(subscriber))  # block on tell
            else:
                raise ValueError("Subscribers Cannot be Empty for Sink")
        else:
            raise ValueError("Subscribers must be provided for Sink")

        self._accounting_actor = kwargs.get('accounting_actor', None)

        is_acct_actor = isinstance(self._accounting_actor, AccountingActor)
        if self._accounting_actor is None or is_acct_actor is False:
            raise AccountingActorNotSuppliedException(
                "Accounting actor not Supplied for Sink")

        self._last_demand = time.time()
        self.register_handler(FlowResult, self.handle_push)

    async def subscribe(self, subscriber):
        await self.tell(subscriber, Subscribe(self))

    async def handle_push(self, message):
        push_time = time.time()

        self.tell(self._publisher, message)
        last_time = time.time() - self._last_accounting
        if last_time > self._accounting_calc_heartbeat:
            push_time = time.time() - push_time
            self.tell(self._accounting_actor, Demand(push_time))
            self._last_accounting = time.time()


class DataFrameSink(BaseActor):
    """
    This sink takes in data and appends to a data frame using specified
    arguments.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class Stage(BaseActor):

    def __init__(self, *args, **kwargs):
        """
        Constructor

        :Keyword Arguments:
            *accounting_calc_heartbeat (double):  Time between demand
            *push_function (function):  The function to use when pushing
            *publisher (PubSub): The output publishers
            *subscriptions (list): A list of PubSubs to subscribe to
            *accounting_actor: The actor for back pressure
        """
        super().__init__(*args, **kwargs)
        self._publisher = kwargs.get('publisher', PubSub())

        is_pub_sub = isinstance(self._publisher, PubSub)
        if self._publisher is None or is_pub_sub is False:
            raise WrongActorException(
                "Publisher Must be an instance of PubSub in Stage"
            )
        
        if self._publisher.get_state() is ActorState.LIMBO:
            self._publisher.start()

        if self._publisher.get_state() is not ActorState.RUNNING:
            raise ActorStateError("Publisher not Started in Stage")
            
        self._accounting_calc_heartbeat = kwargs.get(
            'accounting_calc_heartbeat', 30)
        self._func = kwargs.get('func', None)

        self._push_function = kwargs.get('func', None)        
        if self._push_function is None:
            raise SinkFunctionMissing(
                "push_function must be specified with a Sink"
            )

        self._accounting_actor = kwargs.get('accounting_actor', None)
        is_acct_actor = isinstance(self._accounting_actor, AccountingActor)
        if self._accounting_actor is None or is_acct_actor is False:
            raise AccountingActorNotSuppliedException(
                "Accounting actor not Supplied for Sink")

        if self._accounting_actor.get_state() is ActorState.LIMBO:
            self._accounting_actor.start()
        
        if self._accounting_actor.get_state() is not ActorState.RUNNING:
            raise ActorStateError("Acccounting Actor not running in stage.")

        self._last_accounting = time.time()
        self.register_handler(FlowResult, self.handle_push)

    def subscribe(self, actor):
        self._publisher.subscribe(actor)

    def _do_subscribe(self, message):
        """
        Perform the Subscribe from an actor message
        """
        self.subscribe(message.payload)

    def get_publisher(self):
        """
        Get the publisher.

        :return: The publisher or None
        :rtype: PubSub()
        """
        return self._publisher

    async def handle_push(self, message):
        if self._accounting_actor:
            push_time = time.time()

        result = self._func(message)
        pub = Publish(FlowResult(result))
        await self.tell(self._publisher, pub)
        
        if self._accounting_actor:
            last_time = time.time() - self._last_accounting
            if last_time > self._accounting_calc_heartbeat:
                push_time = time.time() - push_time
                self.tell(self._accounting_actor, Demand(push_time))
                self._last_accounting = time.time()


class FlowControls():

    def __init__(self):
        pass

    def manage_stream(self, source):
        pass
