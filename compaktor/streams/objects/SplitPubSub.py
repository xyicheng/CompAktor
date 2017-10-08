'''
A Stream Based Publisher Subscriber
Created on Oct 7, 2017

@author: aevans
'''

import asyncio
import logging
from janus import Queue as SafeQ
from compaktor.actor.pub_sub import PubSub
from compaktor.message.message_objects import Demand, Publish, Pull,\
    SplitSubscribe, SplitPublish, SplitPull
from compaktor.actor.abstract_actor import AbstractActor


class SplitPubSub(PubSub):
    """
    Streaming Publisher Subscriber. Push requires a specific publisher set to
    be called to force a split.
    """
    def __init__(self, name, provider_q=SafeQ().async_q,
                 loop=asyncio.get_event_loop(), address=None,
                 mailbox_size=1000, inbox=SafeQ().async_q,
                 demand_logic="round_robin"):
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
        self.subscribers = {}
        self.indices = {}
        self.__current_provider = 0
        self.__demand_logic = demand_logic
        self.register_handler(SplitPublish, self.push)
        self.register_handler(SplitPull, self.pull)

    async def subscribe_upstream(self, message):
        """
        Subscribes to an the upstream publisher
        """
        if isinstance(message, SplitSubscribe):
            try:
                payload = message.payload
                split_name = message.split_name
                if split_name in self.subscribers.keys():
                    self.subscribers[split_name].append(payload)
                else:
                    self.subscribers[split_name] = [payload]
                    self.indices[split_name] = 0
            except Exception as e:
                self.handle_fail()

    async def pull(self, message):
        try:
            if isinstance(message, SplitPull):
                split_name = message.split_name()
                result = self.on_pull(message)
                msg = SplitPublish(split_name, result, self)
                await self.push(msg, split_name)
                if self.__demand_logic.lower().strip() == "round_robin":
                    provider = self.providers[self.__current_provider]
                    asyncio.run_coroutine_threadsafe(self.tell(provider, msg))
                    self.__current_provider += 1
                    if self.__current_provider == len(self.providers):
                        self.__current_provider = 0
                elif self.__demand_logic.lower().strip() == "broadcast":
                    for sub in self.subscribers:
                        try:
                            asyncio.run_coroutine_threadsafe(
                                self.tell(sub, msg))
                        except Exception as e:
                            self.handle_fail()
                else:
                    err_msg = "Demand logic in SplitPubSub must be round robin"
                    err_msg += " or broadcast."
                    logging.error(err_msg)
            else:
                msg = "Message to SplitPubSub.pull() must be SplitPull"
                logging.error(msg)
        except Exception as e:
            self.handle_fail()

    def on_pull(self, message):
        logging.error("Should Override Push Function")
        return None

    async def push(self, message, split_name):
        if split_name in self.subscribers.keys():
            subs = self.subscribers[split_name]
            if len(subs) > 0:
                curr_indx = self.indices[split_name]
                if curr_indx < len(subs):
                    pub_to = subs[curr_indx]
                    asyncio.run_coroutine_threadsafe(
                        self.tell(pub_to, message))
                    curr_indx += 1
                    if curr_indx is len(subs):
                        curr_indx = 0
                        self.indices[split_name] = curr_indx
                else:
                    err_msg = "Current Index Greater than Number of Actors{}"
                    err_msg = err_msg.format(curr_indx)
                    logging.error(err_msg)
                    curr_indx = 0
                    self.indices[split_name] = curr_indx
            else:
                err_msg = "No subscribers for {}".format(split_name)
                logging.error(err_msg)
        else:
            err_msg = "Splt Name Does Not Exist {}".format(split_name)
            logging.error(err_msg)
