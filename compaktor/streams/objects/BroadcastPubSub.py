'''
A Stream Based Publisher Subscriber
Created on Oct 7, 2017

@author: aevans
'''

import asyncio
import logging
from janus import Queue as SafeQ
from compaktor.actor.pub_sub import PubSub
from compaktor.message.message_objects import Demand, Publish, Pull
from compaktor.actor.abstract_actor import AbstractActor


class BroadcastPubSub(PubSub):
    """
    Streaming Publisher Subscriber. Push results in a broadcast to all
    subscribers.
    """
    def __init__(self, name, provider_q=None,
                 loop=asyncio.get_event_loop(), address=None,
                 mailbox_size=1000, inbox=None, demand_logic="round_robin"):
        """
        Constructor.  If using a janus queue, only specify round_robin
        demand logic.

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
        :param demand_logic: How to call for demand round_robin or broadcast
        :type demoand_logic: str()
        """
        self.actor_q = inbox
        if self.actor_q is None:
            self.actor_q = SafeQ(loop).async_q
        super().__init__(name, loop, address, mailbox_size, self.actor_q)
        self.provider_q = provider_q
        if self.provider_q is None:
            self.provider_q = SafeQ(loop).async_q
        self.subscribers = {}
        self.__current_provider = 0
        self.__demand_logic = demand_logic
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
            await self.push(message)
            if self.__demand_logic.lower().strip() == "round_robin":
                sub = self.subscribers[self.current_provider]
                asyncio.run_coroutine_threadsafe(self.tell(sub, msg))
                self.__current_provider += 1
                if self.__current_provider > len(self.subscribers):
                    self.__current_provider = 0
            elif self.__demand_logic.lower().strip() == "broadcast":
                for subscriber in self.subscribers:
                    try:
                        asyncio.run_coroutine_threadsafe(self.tell(subscriber, msg))
                    except Exception as e:
                        self.handle_fail()
            else:
                err_msg = "Can only route demand as round robin or broadcast"
                logging.error(err_msg)
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
