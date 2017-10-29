'''
Generic Sink
Created on Oct 7, 2017

@author: aevans
'''

import asyncio
from multiprocessing import cpu_count
from compaktor.actor.base_actor import BaseActor
from compaktor.message.message_objects import Pull, RouteAsk, RouteTell, Push
from compaktor.streams.modules import provider_router


class Sink(BaseActor):
    """
    Sink which should be the final stage in any stream.  Sinks are the o
    in io as opposed to sources which are the i.  The user implements the
    on_push.
    """

    def __init__(self, name, provider_logic="round_robin", providers=[],
                 loop=asyncio.get_event_loop(), address=None,
                 mailbox_size=1000, inbox=None, job_size=cpu_count()):
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
        :param job_size: The size of the job batch
        :type job_size: int
        """
        super().__init__(name, loop, address, mailbox_size, inbox)
        self.register_sink_handlers()
        self.__providers = provider_router.create_provider_router(
            provider_logic, providers)
        self.__job_size = job_size

    def register_sink_handlers(self):
        self.register_handler(Pull, self.__pull)
        self.register_handler(Push, self.__push)

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

    async def __push(self, message):
        print("Sink")
        batch = message.payload
        if batch and len(batch) > 0:
            for res in batch:
                await self.on_push(res)
        await self.tell(self, Pull(None, self))

    async def __pull(self, message):
        """
        Perform a pull request.
        """
        #set the future that needs to be set
        out_message = RouteTell(Pull(self.__job_size, message.sender))
        try:
            await self.__providers.route_tell(out_message)
            await self.tell(self, Pull(None, self))
        except Exception:
            self.handle_fail()

    async def start(self):
        """
        Start a number of pull requests up to the max number.
        """
        super().start()
        try:
            print("Starting")
            await self.tell(self, Pull(None, self))
        except Exception:
            self.handle_fail()
