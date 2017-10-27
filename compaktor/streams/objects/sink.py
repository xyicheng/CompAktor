'''
Generic Sink
Created on Oct 7, 2017

@author: aevans
'''

import asyncio
import logging
from multiprocessing import cpu_count
from compaktor.actor.base_actor import BaseActor
from compaktor.message.message_objects import Publish, Push, Pull, RouteAsk
from compaktor.streams.modules import provider_router


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
        self.register_sink_handlers()
        self.__providers = provider_router.create_provider_router(
            provider_logic, providers)
        self.__concurrency = concurrency

    def register_sink_handlers(self):
        self.register_handler(Pull, self.__pull)

    def add_provider(self, actor):
        """
        Add a provider to the router

        :param actor: The actor to add to the router
        :type actor: BaseActor
        """
        provider_router.add_provider(self.__providers, actor)

    async def on_push(self, payload):
        """
        Handle payload. Overwrite

        :param payload: The payload returned by ask
        :type payload: object
        """
        pass

    async def __pull(self, message):
        """
        Perform a pull request.
        """
        #set the future that needs to be set
        out_message = RouteAsk(message)
        try:
            oresult = await self.__providers.route_ask(out_message)
            await self.on_push(oresult)
            await self.tell(self, message)
        except Exception:
            self.handle_fail()

    def start(self):
        """
        Start a number of pull requests up to the max number.
        """
        super().start()
        for i in range(0, self.__concurrency):
            try:
                self.loop.run_until_complete(self.tell(self, Pull))
            except Exception:
                self.handle_fail()
