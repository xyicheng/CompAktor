'''
Generic Sink
Created on Oct 7, 2017

@author: aevans
'''

import asyncio
import logging
from multiprocessing import cpu_count
from compaktor.actor.base_actor import BaseActor
from compaktor.message.message_objects import Publish, Push, Pull, Subscribe
from compaktor.routing.round_robin import RoundRobinRouter
from compaktor.routing.balancing import BalancingRouter
import pdb


class Sink(BaseActor):
    """
    Sink which should be the final stage in any stream.  Sinks are the o
    in io as opposed to sources which are the i.  The user implements the
    on_push.
    """

    def __init__(self, name, provider_logic="round_robin", providers=[],
                 loop=asyncio.get_event_loop(), address=None,
                 mailbox_size=1000, inbox=None, concurrency=cpu_count()):
        """
        Sink Constructor.

        :param name: The name of the sink
        :type name: str()
        :param loop: The loop to run the sink on
        :type loop: AbstractEventLoop()
        :param address: Address for the actors
        :type address: str()
        :param mailbox_size: Maximum size of the mailbox
        :type mailbox_size: int()
        :param inbox: Mailbox queue to use must have get() and put() methods
        :type inbox: Queue()
        """
        super().__init__(name, loop, address, mailbox_size, inbox)
        self.set_handlers()
        self.__providers = None
        self.__concurrency = concurrency
        self.create_provider_router(provider_logic, providers)

    def set_handlers(self):
        self.register_handler(Publish, self.__push)
        self.register_handler(Push, self.__push)

    def create_provider_router(self, provider_logic, providers):
        try:
            logic = provider_logic.lower().strip()
            if logic == "round_robin":
                self.__providers = RoundRobinRouter()
            elif logic == "balancing":
                self.__providers = BalancingRouter()
        except Exception:
            self.handle_fail()

    def add_source(self, actor):
        """
        Add a source to the provider router.

        :param actor: The provider actor
        :type actor: AbstractActor()
        """
        if self.__providers:
            sub = Subscribe(actor, self)
            self.loop.run_until_complete(self.__providers.route_tell(sub))
            if len(self.__providers.actor_set) <= self.__concurrency:
                message = Publish(
                    Pull(None, self), self)
                self.loop.run_until_complete(
                    self.__providers.route_tell(message))

    async def on_push(self, payload):
        """
        Override. Handle the payload

        :param payload: The payload from the message
        :type payload: object
        """
        pass

    async def __push(self, message):
        """
        Handle a push request

        :param message: The message from the request
        :type message: Message()
        """
        try:
            if message and message.sender:
                sender = message.sender
                load = message.payload
                await self.on_push(load)
                out_message = Pull(None, self)
                await self.tell(sender, out_message)
            else:
                logging.error(
                    "Message for Push Cannot Be None and Must Have a Sender")
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
