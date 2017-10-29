'''
Balancing Publisher Subscriber
Created on Oct 7, 2017

@author: aevans
'''

import asyncio
from multiprocessing import cpu_count
from queue import Queue as PyQueue
from compaktor.message.message_objects import Pull, Push, RouteAsk, RouteTell
from compaktor.actor.base_actor import BaseActor
from compaktor.streams.modules import provider_router
from compaktor.multiprocessing.pool import multiprocessing_pool

class NodePubSub(BaseActor):
    """
    A standard graph stage pub sub.  This pub sub receives pull requests which
    wait on a push to then complete a task and send back to the sender.
    Non-blocking io should assure maximal concurrency.
    """

    def __init__(self, name, provider_logic="round_robin", providers=[],
                 loop=asyncio.get_event_loop(), address=None, executor=None,
                 mailbox_size=1000, inbox=None, job_size=cpu_count()):
        """
        Constructor

        :param name: Name of the actor
        :type name: str
        :param provider_logic: The logic to use in requesting a batch
        :type provider_logic: str()
        :param providers: The providers set
        :type providers: list()
        :param loop: Asyncio loop for the actor
        :type loop: AbstractEventLoop
        :param address: Address for the actor
        :type address: str
        :param executor: The executor to use
        :type executor: Futures executor
        :param mailbox_size: Size of the mailbox
        :type mailbox_size: int
        :param inbox: Actor inbox
        :type inbox: asyncio.Queue
        :param job_size: The maximum number of records to process in a job
        :type job_size: int
        """
        super().__init__(name, loop, address, mailbox_size, inbox)
        self.__mailbox_size = mailbox_size
        self.__job_size = job_size
        self.__result_q = asyncio.Queue(maxsize=mailbox_size)
        self.__providers = provider_router.create_provider_router(
            provider_logic, providers)
        self.__current_provider = 0
        self.__executor = executor
        self.register_node_handlers()

    def register_node_handlers(self):
        """
        Register handlers for message pushing
        """
        self.register_handler(Pull, self.__push)
        self.register_handler(Push, self.__pull)

    async def on_pull(self, el):
        """
        Overwrite. Handle each element in a batch.

        :param el: The element to handle
        :type el: object
        """
        pass

    def add_actor(self, actor):
        """
        Add an actor to the router.

        :param actor: The actor to add
        :type actor: AbstractActor
        """
        self.__providers.add_actor(actor)

    async def __pull(self, message):
        """
        Pull Function servicing a push.

        :param message: The message from push
        :type message: object
        """
        try:
            #submit a request for a batch of at most n size
            batch = message.payload
            if batch:
                if batch and len(batch) > 0:
                    for el in batch:
                        res = await self.on_pull(el)
                        await self.__result_q.put(res)
                else:
                    await asyncio.sleep(1)
        except Exception:
            self.handle_fail()

    async def __push(self, message):
        """
        Push function servicing a pull.

        :param message: The message from pull
        :type message: object
        """
        out_batch = []
        try:
            num_els = message.payload
            i = 0
            while i < num_els and self.__result_q.empty() is False:
                print("Getting")
                res = await self.__result_q.get()
                out_batch.append(res)
                i += 1
            req_size = self.__job_size - self.__mailbox_size
            gmsg = Pull(req_size, self)
            pmsg = Push(out_batch, self)
            await self.__providers.route_tell(RouteTell(gmsg, self))
            await self.tell(message.sender, pmsg)
        except Exception:
            self.handle_fail()
