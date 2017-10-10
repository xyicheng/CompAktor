'''
Balancing Publisher Subscriber
Created on Oct 7, 2017

@author: aevans
'''

import asyncio
import logging
from queue import Queue as PyQueue
from compaktor.actor.pub_sub import PubSub
from compaktor.message.message_objects import Publish, Pull,\
                                                DeSubscribe, Subscribe
from compaktor.actor.abstract_actor import AbstractActor


class NodePubSub(PubSub):
    """
    A standard graph stage pub sub.  This pub sub receives pull requests which
    wait on a push to then complete a task and send back to the sender.
    Non-blocking io should assure maximal concurrency.
    """

    def __init__(self, name, providers, loop=asyncio.get_event_loop(),
                 address=None, mailbox_size=1000, inbox=None,
                 empty_demand_logic = "broadcast"()):
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
        """
        super().__init__(name, loop, address, mailbox_size, inbox)
        self.subscribers = []
        self.__providers = providers
        self.__current_provider = 0
        if self.__providers is None or len(self.__providers) is 0:
            raise ValueError("Initial providers must be supplied")
        self.register_handler(Publish, self.__pull)
        self.register_handler(Pull, self.__push) 
        self.register_handler(DeSubscribe, self.__de_subscribe_upstream)
        self.register_handler(Subscribe, self.__subscribe_upstream)
        self.__task_q = PyQueue()
        self.__empty_logic = empty_demand_logic
        self.__iter_out = []
        self.run_on_empty()

    def run_on_empty(self):
        if self.__empty_logic == "broadcast":
            for provider in self.__providers:
                asyncio.run_coroutine_threadsafe(self.tell(provider, Pull(None, self)))
        else:
            if len(self.__providers) > 0:
                if self.__current_provider >= len(self.__providers):
                    self.__current_provider = 0
                prov = self.__providers[self.__current_provider]
                asyncio.run_coroutine_threadsafe(self.tell(prov, Pull(None, self)))
                self.__current_provider += 1

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
                    if self.__iter_out and len(self.__iter_out) > 0:
                        result = self.__iter_out.pop(0)
                    else:
                        result = self.on_pull(task)
                        if result and isinstance(result, list):
                            self.__iter_out = result
                            if len(result) > 0:
                                result = self.__iter_out.pop(0)
                            else:
                                result = None
                    msg = Publish(result, self)
                    self.provider_q.put(msg)
                    await self.tell(sender, Pull(None, self))
        except Exception as e:
            self.handle_fail()

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
