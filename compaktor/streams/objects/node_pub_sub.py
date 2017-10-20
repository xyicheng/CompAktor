'''
Balancing Publisher Subscriber
Created on Oct 7, 2017

@author: aevans
'''

import asyncio
import logging
from multiprocessing import cpu_count
from queue import Queue as PyQueue
from compaktor.actor.pub_sub import PubSub
from compaktor.message.message_objects import Publish, Pull,\
                                                DeSubscribe, Subscribe,\
    TaskMessage, Tick
from compaktor.actor.abstract_actor import AbstractActor
from compaktor.routing.round_robin import RoundRobinRouter
from compaktor.streams.objects.stage_task_actor import TaskActor
from compaktor.state.actor_state import ActorState


class NodePubSub(PubSub):
    """
    A standard graph stage pub sub.  This pub sub receives pull requests which
    wait on a push to then complete a task and send back to the sender.
    Non-blocking io should assure maximal concurrency.
    """

    def __init__(self, name, providers=[], loop=asyncio.get_event_loop(),
                 address=None, mailbox_size=1000, inbox=None,
                 empty_demand_logic = "broadcast", concurrency=cpu_count(),
                 tick_delay=120):
        """
        Constructor

        :param name: Name of the actor
        :type name: str()
        :param loop: Asyncio loop for the actor
        :type loop: AbstractEventLoop()
        :param address: Address for the actor
        :type address: str()
        :param mailbox_size: Size of the mailbox
        :type mailbox_size: int)
        :param inbox: Actor inbox
        :type inbox: asyncio.Queue()
        :param empty_demand_logic: round_robin or broadcast
        :type empty_demand_logic: str()
        :param concurrency: Number concurrent tasks to run
        :type concurrency: int()
        """
        super().__init__(name, loop, address, mailbox_size, inbox)
        self.subscribers = []
        self.__providers = providers
        self.__current_provider = 0
        self.__task_q = PyQueue()
        self.__empty_logic = empty_demand_logic
        self.__result_q = PyQueue()
        self.router = RoundRobinRouter()
        self.create_router(concurrency)
        self.set_handlers()
        self.tick_delay = tick_delay
        self.__concurrency = concurrency
        self.__pull_tick()
        self.register_handler(Tick, self.__pull_tick())

    def set_handlers(self):
        """
        Set the handlers for the actor
        """
        self.register_handler(Publish, self.pull)
        self.register_handler(Pull, self.push)
        self.register_handler(DeSubscribe, self.__de_subscribe_upstream)
        self.register_handler(Subscribe, self.__subscribe_upstream)
        self.register_handler(TaskMessage, self.__append_result)

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

    def start(self):
        super().start()
        if len(self.__providers) > 0:
            for i in range(0, self.__concurrency):
                if self.__empty_logic == "broadcast":
                    for provider in self.__providers:
                        asyncio.run_coroutine_threadsafe(
                            self.tell(provider, Pull(None, self)))
                else:
                    if len(self.__providers) > i:
                        if i < len(self.__providers):
                            lprv = len(self.__providers)
                            if self.__current_provider >= lprv:
                                self.__current_provider = 0
                            prov = self.__providers[self.__current_provider]
                            asyncio.run_coroutine_threadsafe(
                                self.tell(prov, Pull(None, self)))
                            self.__current_provider += 1

    def __call_tick(self):
        self.loop.run_until_complete(self.tell(self, Tick(None, self)))

    def __do_pull_tick(self):
        """
        Do a pull tick
        """
        try:
            if self.__task_q.full() is False:
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
                self.loop.call_later(self.tick_delay, self.__call_tick())
        except Exception as e:
            self.handle_fail()

    def __pull_tick(self):
        self.loop.call_later(self.tick_delay, self.__do_pull_tick())

    async def __subscribe_upstream(self, message):
        """
        Subscribes to an the upstream publisher

        :param message: The provided Subscribe message
        :type message: Subscribe()
        """
        payload = message.payload
        if isinstance(payload, AbstractActor):
            if payload not in self.subscribers:
                self.subscribers.append(payload)
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
                    if actor in self.subscribers:
                        self.subscribers.remove(actor)
                        if self.__current_provider >= len(self.subscribers):
                            self.__current_provider = 0
        except Exception as e:
            self.handle_fail()

    async def __append_result(self, message):
        """
        Append an incoming result to the result queue.

        :param message:  The incoming message
        :type message:  Message()
        """
        try:
            if isinstance(message, TaskMessage):
                payload = message.payload
                self.__result_q.put_nowait(payload)
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
                task = self.__task_q.get()
                if task:
                    sender = message.sender
                    result = None
                    if self.__result_q.empty() is False:
                        try:
                            result = self.__result_q.get_nowait()
                        except Exception as e:
                            pass                        
                    
                    if self.__result_q.full() is False:
                        await self.tell(self.router, TaskMessage(message))
                        await self.__signal_provider()

                    if result is not None:
                        sender = message.sender
                        msg = Publish(result, self)
                        await self.tell(sender, msg)
        except Exception as e:
            self.handle_fail()

    async def __signal_provider(self):
        try:
            prov = self.providers[self.__current_provider]
            await self.tell(prov, Pull(None, self))
        except Exception as e:
            self.handle_fail()
        self.__current_provider += 1
        if self.__current_provider >= len(self.__providers):
            self.__current_provider  = 0

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
