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
                                                TaskMessage, Tick, Push,\
                                                RouteTell
from compaktor.actor.abstract_actor import AbstractActor
from compaktor.routing.round_robin import RoundRobinRouter
from compaktor.streams.objects.stage_task_actor import TaskActor
from compaktor.state.actor_state import ActorState
import pdb


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
        self.register_handler(Tick, self.__pull_tick)
        self.subscribers = []
        self.__providers = providers
        self.__current_provider = 0
        self.__task_q = PyQueue()
        self.__empty_logic = empty_demand_logic
        self.__result_q = PyQueue()
        self.router = None
        self.create_router(concurrency)
        self.set_handlers()
        self.tick_delay = tick_delay
        self.__concurrency = concurrency
        self.__pull_tick()

    def set_handlers(self):
        """
        Set the handlers for the actor
        """
        self.register_handler(Publish, self.__push)
        self.register_handler(Push, self.__push)
        self.register_handler(Pull, self.__pull)
        self.register_handler(Tick, self.__do_pull_tick)
        self.register_handler(DeSubscribe, self.__de_subscribe_upstream)
        self.register_handler(Subscribe, self.__subscribe_upstream)
        self.register_handler(TaskMessage, self.__append_result)

    def create_router(self, concurrency):
        """
        Create the initial router with existing actors.

        :param concurrency: Number of routers to start
        :type concurrency: int()
        """
        if self.router is None:
            self.router = RoundRobinRouter()
            self.router.start()
            for i in range(0, concurrency):
                try:
                    pyname = __name__
                    router_name = "{}_{}".format(pyname, i)
                    ta = TaskActor(name=router_name, on_call=self.on_pull,
                                   loop=self.loop)
                    ta.start()
                    self.router.add_actor(ta)
                except Exception:
                    self.handle_fail()

    def start(self):
        """
        Start the actor  
        """
        super().start()
        if len(self.__providers) > 0:
            for i in range(0, self.__concurrency):
                if self.__empty_logic == "broadcast":
                    for provider in self.__providers:
                        self.loop.run_until_complete(
                            self.tell(provider, Pull(None, self)))
                else:
                    if len(self.__providers) > i:
                        if i < len(self.__providers):
                            lprv = len(self.__providers)
                            if self.__current_provider >= lprv:
                                self.__current_provider = 0
                            prov = self.__providers[self.__current_provider]
                            self.loop.run_until_complete(
                                self.tell(prov, Pull(None, self)))
                            self.__current_provider += 1

    def call_tick(self):
        """
        Perform a tick call.
        """
        def handle_tick():
            """
            Handle the tick
            """
            try:
                asyncio.run_coroutine_threadsafe(
                    self.tell(self, Tick(None, self)), loop=self.loop)
            except Exception:
                self.handle_fail()
        try:
            self.loop.call_later(
                delay = self.tick_delay, callback=handle_tick)
        except Exception:
            self.handle_fail()

    async def __do_pull_tick(self, message=None):
        """
        Do a pull tick

        :param message: The message for the pull tick
        :type message: Message()
        """
        try:
            if self.__task_q.full() is False:
                if self.__providers and len(self.__providers) > 0:
                    for provider in self.__providers:
                        if isinstance(provider, AbstractActor):
                            if provider.get_state() != ActorState.RUNNING:
                                self.__providers.remove(provider)
                    if len(self.__providers) > 0:
                        sender = self.__providers[self.__current_provider]
                        await self.tell(sender,Pull(None, self))
                        self.__current_provider += 1
                        if self.__current_provider >= len(self.__providers):
                            self.__current_provider = 0
                    else:
                        self.__current_provider = 0
                else:
                    self.__current_provider = 0
        except Exception:
            self.handle_fail()
        try:
            if self.get_state() != ActorState.TERMINATED:
                self.call_tick()
        except Exception:
            self.handle_fail()

    def __pull_tick(self):
        """
        Perform a periodic pull.
        """
        self.loop.run_until_complete(self.__do_pull_tick())

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
        except Exception:
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
        except Exception:
            self.handle_fail()

    async def __pull(self, message):
        """
        The pull message.

        :param message: The Pull message
        :type message: Pull()
        """
        try:
            await self.__signal_provider()
            if isinstance(message, Pull):
                if self.__task_q.empty():
                    self.run_on_empty()
                task = self.__task_q.get()
                if task:
                    if isinstance(task, Push):
                        task = task.payload
                    await self.tell(
                        self.router, RouteTell(TaskMessage(task, message.sender, self)))
        except Exception:
            self.handle_fail()

    async def __signal_provider(self):
        """
        Signal the provider.
        """
        try:
            prov = self.__providers[self.__current_provider]
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

    async def __push(self, message):
        """
        The push function.
        """
        if isinstance(message, Publish):
            self.__task_q.put_nowait(message.payload)
