'''
Balancing Publisher Subscriber
Created on Oct 7, 2017

@author: aevans
'''

import asyncio
from multiprocessing import cpu_count
from queue import Queue as PyQueue
from compaktor.message.message_objects import Pull, DeSubscribe, Subscribe,\
                                                Tick, Push, Publish
from compaktor.routing.round_robin import RoundRobinRouter
from compaktor.routing.balancing import BalancingRouter
from compaktor.streams.objects.stage_task_actor import TaskActor
from compaktor.actor.base_actor import BaseActor
import logging
import pdb


class NodePubSub(BaseActor):
    """
    A standard graph stage pub sub.  This pub sub receives pull requests which
    wait on a push to then complete a task and send back to the sender.
    Non-blocking io should assure maximal concurrency.
    """

    def __init__(self, name, provider_logic="round_robin", providers=[],
                 loop=asyncio.get_event_loop(), address=None,
                 mailbox_size=1000, inbox=None,
                 empty_demand_logic = "broadcast", concurrency=cpu_count(),
                 tick_delay=120, routing_logic = "round_robin"):
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
        :param routing_logic: Type of router to create
        :type routing_logic: str()
        """
        super().__init__(name, loop, address, mailbox_size, inbox)
        self.__concurrency = concurrency
        self.__routing_logic = routing_logic
        self.__providers = None
        self.__current_provider = 0
        self.__task_q = PyQueue()
        self.__empty_logic = empty_demand_logic
        self.__result_q = PyQueue()
        self.__create_provider_router(provider_logic, providers)
        self.set_handlers()
        self.tick_delay = tick_delay

    def set_handlers(self):
        """
        Register handlers for message pushing
        """
        self.register_handler(Push, self.__push)
        self.register_handler(Pull, self.__pull)

    def __create_provider_router(self, logic, actor_set=[]):
        """
        Create a provider router. 
        """
        if logic.lower().strip() == "round_robin" or\
            logic.lower().strip() == "broadcast":
            self.providers = RoundRobinRouter(
                address=self.address, actors=actor_set)
        elif logic.lower().strip() == "balancing":
            self.providers = BalancingRouter(
                address=self.address, actors=actor_set)
        if self.providers and len(actor_set) < self.__concurrency:
            rem_conc = self.__concurrency - len(actor_set)
            actor_addr = self.providers.address
            for i in range(0, rem_conc):
                actor = TaskActor(
                    name=i, on_call=self.on_pull, address=actor_addr)
                self.providers.add_actor(actor)

    def add_source(self, actor):
        """
        Add a source to the provider router.

        :param actor: The provider actor
        :type actor: AbstractActor()
        """
        if self.__providers:
            sub = Subscribe(actor, self)
            self.__providers.route_tell(sub)
            if len(self.__providers.actor_set) <= self.__concurrency:
                message = Publish(
                    Pull(None, self), self)
                self.loop.run_until_complete(
                    self.__providers.route_tell(message))

    async def on_pull(self, payload):
        """
        Override. Handle the payload.

        :param payload: The payload from the message
        :type payload: Object
        """
        return payload

    async def __pull(self, message):
        """
        Perform a pull request.

        :param message: The calling message
        :type message: Pull()
        """
        async def get_from_queue():
            return self.__task_q.get()
        try:
            if message and message.sender:
                load = await get_from_queue()
                oresult = await self.on_pull(load)
                if oresult:
                    sender = message.sender
                    await self.tell(
                        sender, oresult)
            else:
                logging.error("Message For PubSub was None")
        except Exception:
            self.handle_fail()

    async def __push(self, message):
        """
        Perform a push request.

        :param message: The calling message
        :type message: Push() 
        """
        try:
            self.__result_q.put(message)
        except Exception:
            self.handle_fail()

    def start(self):
        """
        Specialized start method that creates an initial
        set of pull requests based on the initial size
        of the router.
        """
        super().start()
        if self.__providers:
            router_size = len(self.__providers.actor_set)
            if router_size > 0:
                for i in range(0, self.__concurrency):
                    message = Publish(
                        Pull(None, self), self)
                    self.loop.run_until_complete(
                        self.__providers.route_tell(message))
