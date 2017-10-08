'''
Balancing Publisher Subscriber
Created on Oct 7, 2017

@author: aevans
'''

import asyncio
import logging
from janus import Queue as SafeQ
from compaktor.actor.pub_sub import PubSub
from compaktor.message.message_objects import Demand, Publish, Pull,\
                                                DeSubscribe, Subscribe
from compaktor.actor.abstract_actor import AbstractActor


class BalancingPubSub(PubSub):
    """
    A balancing pubsub uses.  A balancing pubsub uses the provider_q.
    The balancing pubsub uses a single queue for providers.  The user
    must ensure that this queue is the inbox for every provider.  Do 
    not use with too many providers.

    The user also implements the on_pull function to perform actions on pull.
    """

    def __init__(self, name, provider_q=SafeQ().async_q,
                 loop=asyncio.get_event_loop(), address=None,
                 mailbox_size=1000, inbox=SafeQ().async_q):
        """
        Constructor

        :param name: Name of the actor
        :type name: str()
        :param provider_q: Queue for the provider
        :type provider_q: janus.Queue()
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
        self.provider_q = provider_q
        self.subscribers = []
        self.__current_provider = 0
        self.register_handler(Publish, self.__push)
        self.register_handler(Pull, self.__pull)
        self.register_handler(DeSubscribe, self.__de_subscribe_upstream)
        self.register_handler(Subscribe, self.__subscribe_upstream)

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
            result = self.on_pull(message)
            msg = Publish(result, self)
            await self.push(msg)
            self.provider_q.put(msg)
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
        for subscriber in self.subscribers:
            try:
                asyncio.run_coroutine_threadsafe(self.tell(subscriber, message))
            except Exception as e:
                self.handle_fail()
