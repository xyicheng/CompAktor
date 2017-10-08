'''
Balancing Publisher Subscriber
Created on Oct 7, 2017

@author: aevans
'''

import asyncio
import logging
from janus import Queue as SafeQ
from compaktor.actor.pub_sub import PubSub
from compaktor.message.message_objects import Demand, Publish, Pull
from compaktor.actor.abstract_actor import AbstractActor


class BalancingPubSub(PubSub):
    """
    A balancing pulsub uses 
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
        self.providers = provider_q
        self.subscribers = []
        self.__current_provider = 0
        self.register_handler(Publish, self.push)
        self.register_handler(Pull, self.pull)

    async def subscribe_upstream(self, message):
        """
        Subscribes to an the upstream publisher
        """
        payload = message.payload
        if isinstance(payload, AbstractActor):
            if payload not in self.subscribers:
                self.subscribers.append(payload)
        else:
            msg = "Can Only Subscribe Object of Abstract Actor to StreamPubSub"
            logging.error(msg)

    async def pull(self, message):
        try:
            result = self.on_pull(message)
            msg = Publish(result, self)
            await self.push(msg)
        except Exception as e:
            self.handle_fail()

    def on_pull(self, message):
        logging.error("Should Override Push Function")
        return None

    async def push(self, message):
        for subscriber in self.subscribers:
            try:
                asyncio.run_coroutine_threadsafe(self.tell(subscriber, message))
            except Exception as e:
                self.handle_fail()
