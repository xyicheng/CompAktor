'''
Balancing Publisher Subscriber
Created on Oct 7, 2017

@author: aevans
'''

import asyncio
from multiprocessing import cpu_count
from queue import Queue as PyQueue
from compaktor.message.message_objects import Pull, Push
from compaktor.actor.base_actor import BaseActor
from compaktor.streams.modules import provider_router


class NodePubSub(BaseActor):
    """
    A standard graph stage pub sub.  This pub sub receives pull requests which
    wait on a push to then complete a task and send back to the sender.
    Non-blocking io should assure maximal concurrency.
    """

    def __init__(self, name, provider_logic="round_robin", providers=[],
                 loop=asyncio.get_event_loop(), address=None,
                 mailbox_size=1000, inbox=None,
                 concurrency=cpu_count()):
        """
        Constructor

        :param name: Name of the actor
        :type name: str
        :param loop: Asyncio loop for the actor
        :type loop: AbstractEventLoop
        :param address: Address for the actor
        :type address: str
        :param mailbox_size: Size of the mailbox
        :type mailbox_size: int
        :param inbox: Actor inbox
        :type inbox: asyncio.Queue
        :param empty_demand_logic: round_robin or broadcast
        :type empty_demand_logic: str
        :param concurrency: Number concurrent tasks to run
        :type concurrency: int
        :param routing_logic: Type of router to create
        :type routing_logic: str
        """
        super().__init__(name, loop, address, mailbox_size, inbox)
        self.__concurrency = concurrency
        self.__task_q = PyQueue()
        self.__providers = provider_router.create_provider_router(
            provider_logic, providers)
        self.__current_provider = 0
        self.register_node_handlers()

    def register_node_handlers(self):
        """
        Register handlers for message pushing
        """
        self.register_handler(Push, self.__do_pull)
        self.register_handler(Pull, self.__pull)

    def add_provider(self, actor):
        """
        Add a provider to the router

        :param actor: The actor to add to the router
        :type actor: BaseActor
        """
        provider_router.add_provider(self.__providers, actor)

    async def __do_pull(self, message):
        """
        Perform a downstream pull to be put into the queue.

        :param message: The pull message
        :type message: Message
        """
        out_message = Pull(None, self)
        #ask for data from beneath this actor
        oresult = None
        try:
            if self.__task_q.full() is False:
                oresult = await self.__providers.route_ask(out_message)
                self.__task_q.put(oresult)
                rmsg = Push(None, self)                
                await self.tell(self, rmsg)
            else:
                #wait a bit and call
                out_message = Pull(None, self)
                await asyncio.sleep(0.1, loop=self.loop)
                await self.tell(self, out_message)
        except Exception:
            self.handle_fail()

    async def on_push(self, payload):
        """
        Handle payload. Overwrite.

        :param payload: The payload returned by ask
        :type payload: object
        """
        return None 

    async def __pull(self, message):
        """
        Perform a pull request.

        :param message: The message to pull
        :type message: Message
        """        
        #set the future that needs to be set
        omsg = Pull(None, self)
        oresult = None
        try:
            await self.tell(self, omsg)
            oresult = self.__task_q.get()
            oresult = await self.on_push(oresult)
            await self.tell(self, message)
        except Exception:
            self.handle_fail()
        return oresult

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
