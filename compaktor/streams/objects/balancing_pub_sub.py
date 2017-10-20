'''
Balancing Publisher Subscriber
Created on Oct 7, 2017

@author: aevans
'''

import asyncio
import logging
from multiprocessing import cpu_count
from queue import Queue as PyQueue
from janus import Queue as SafeQ
from compaktor.actor.pub_sub import PubSub
from compaktor.message.message_objects import Publish, Pull,\
                                                DeSubscribe, Subscribe,\
    TaskMessage
from compaktor.actor.abstract_actor import AbstractActor
from abc import abstractmethod
from compaktor.streams.objects.stage_task_actor import TaskActor
from compaktor.state.actor_state import ActorState


class BalancingPubSub(PubSub):
    """
    A balancing pubsub uses the provider_q to push results.
    The balancing pubsub uses a single queue for providers.  The user
    must ensure that this queue is the inbox for every provider.  Do 
    not use with too many providers.

    The user also implements the on_pull function to perform actions on pull.
    """

    def __init__(self, name, providers=[], push_q=SafeQ().async_q,
                 loop=asyncio.get_event_loop(), address=None,
                 mailbox_size=1000, inbox=SafeQ().async_q,
                 empty_demand_logic="broadcast", concurrency=cpu_count(),
                 routing_logic="round_robin", tick_delay=120):
        """
        Constructor

        :param name: Name of the actor
        :type name: str()
        :param push_q: Queue for the provider
        :type push_q: janus.Queue()
        :param loop: Asyncio loop for the actor
        :type loop: AbstractEventLoop()
        :param address: Address for the actor
        :type address: str()
        :param mailbox_size: Size of the mailbox
        :type mailbox_size: int)
        :param inbox: Actor inbox
        :type inbox: asyncio.Queue()
        :param concurrency: The max concurrency in the system
        :type concurrency: int()
        :param routing_logic: round_robin or broadcast
        :param routing_logic: str()
        """
        super().__init__(name, loop, address, mailbox_size, inbox)
        self.push_q = push_q
        self.result_q = PyQueue()
        self.__subscribers = []
        self.__providers = providers
        self.__current_provider = 0
        self.__task_q = PyQueue()
        self.__empty_demand_logic = empty_demand_logic
        self.__concurrency = concurrency
        self.__routing_logic = routing_logic
        self.set_handlers()
        self.tick_delay = tick_delay
        self.run_on_empty(self.__concurrency)
        self.__pull_tick()

    def set_handlers(self):
        """
        Set the node handlers
        """
        self.register_handler(Publish, self.__pull)
        self.register_handler(Pull, self.__push) 
        self.register_handler(DeSubscribe, self.__de_subscribe_upstream)
        self.register_handler(Subscribe, self.__subscribe_upstream)
        self.register_handler(TaskMessage, self.__append_result)

    def __do_pull_tick(self):
        """
        Do a pull tick
        """
        try:
            if self._task_q.full() is False:
                if len(self.__providers) > 0:
                    sender = self.__providers[self.__current_provider]
                    self.loop.run_until_complete(
                        self.tell(self,Pull(None, sender)))
                    self.__current_provider += 1
                    if self.__current_provider >= len(self.__providers):
                        self.__current_provider = 0
                        
                else:
                    self.__current_provider = 0
        except Exception as e:
            self.handle_fail()

        try:
            if self.get_state() != ActorState.TERMINATED:
                self.loop.call_later(self.tick_delay, self.__do_pull_tick())
        except Exception as e:
            self.handle_fail()

    def __pull_tick(self):
        self.loop.call_later(self.tick_delay, self.__do_pull_tick())

    def create_router(self, concurrency):
        """
        Create the initial router with existing actors.

        :param concurrency: Number of routers to start
        :type concurrency: int()
        """
        for i in range(0, concurrency):
            try:
                pyname = __name__
                router_name = "{}_{}".format(pyname, i)
                ta = TaskActor(name=router_name, on_call=self.on_pull,
                               loop=self.loop)
                ta.start()
                self.router.add_actor(ta)
            except Exception as e:
                self.handle_fail()

    def run_on_empty(self, concurrency):
        for i in range(0, concurrency):
            if self.__empty_logic == "broadcast":
                for provider in self.__providers:
                    asyncio.run_coroutine_threadsafe(self.tell(provider, Pull(None, self)))
            else:
                if len(self.__providers) > i:
                    if self.__current_provider >= len(self.__providers):
                        self.__current_provider = 0
                    prov = self.__providers[self.__current_provider]
                    asyncio.run_coroutine_threadsafe(self.tell(prov, Pull(None, self)))
                    self.__current_provider += 1

    async def __append_result(self, message):
        """
        Append a result to the result queue to be grabbed and used

        :param message: The TaskMessage from the router
        :type message: TaskMessage
        """
        try:
            if isinstance(message, TaskMessage):
                payload = message.payload
                self.result_q.put_nowait(payload)
        except Exception as e:
            self.handle_fail()

    async def __subscribe_upstream(self, message):
        """
        Subscribes to an the upstream publisher

        :param message: The provided Subscribe message
        :type message: Subscribe()
        """
        payload = message.payload
        if isinstance(payload, AbstractActor):
            if payload not in self.__subscribers:
                self.__subscribers.append(payload)
        else:
            msg = "Can Only Subscribe Object of Abstract Actor to StreamPubSub"
            logging.error(msg)

    async def __de_subscribe_upstream(self, message):
        """
        De-subscribe from the upstream actors.

        :param message:  The DeSubscribe message
        :type message: DeSubscribe() 
        """
        try:
            if isinstance(message, DeSubscribe):
                actor = message.payload
                if isinstance(actor, AbstractActor):
                    if actor in self.__subscribers:
                        self.__subscribers.remove(actor)
                        if self.__current_provider >= len(self.__subscribers):
                            self.__current_provider = 0
        except Exception as e:
            self.handle_fail()

    async def pull(self, message):
        """
        The pull message.

        :param message: The Pull message
        :type message: Pull()
        """
        try:
            if isinstance(message, Pull):
                if self.__task_q.empty():
                    self.run_on_empty()
                elif self.__task_q.full() is False:
                    task = self.__task_q.get()
                    if task:
                        await self.tell(self.router, TaskMessage(message))
                if self.result_q.empty() is False:
                    sender = message.sender
                    result = self.result_q.get_nowait()
                    self.push_q.put(result)
                    await self.tell(sender, Pull(None, self))
        except Exception as e:
            self.handle_fail()

    @abstractmethod
    def on_pull(self, message):
        """
        User implemented function to handle incoming methods.
        """
        logging.error("Should Override Push Function")
        return None

    async def push(self, message):
        """
        The push function.
        """
        if isinstance(message, Publish):
            self.__task_q.put_nowait(message)
