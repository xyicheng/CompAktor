'''
Source for obtaining information
Created on Oct 7, 2017

@author: aevans
'''

import asyncio
import logging
from multiprocessing import cpu_count
from compaktor.actor.pub_sub import PubSub
from compaktor.message.message_objects import Pull, Subscribe, DeSubscribe,\
    Publish, PullQuery, Push
from abc import abstractmethod
import pdb


class Source(PubSub):
    """
    Source.  Can be chained to provide information.  Demand typically comes in
    via round robin logic.  Nodes subscribe to the source.  The user implements
    the on_pull method in their extended Source.
    """

    def __init__(self, name, tasks=cpu_count(),loop=asyncio.get_event_loop(),
                 address=None, mailbox_size=1000, inbox=None,
                 routing_logic="round_robin", subscribers=[]):
        """
        Constructor

        :param name: The name of the source. Should be uniques.
        :type name: str()
        :param tasks: The number of concurrent tasks to run
        :type tasks: int()
        :param loop: The loop for the Source
        :type loop: AbstractEventLoop()
        :param address: The unique address for the Source
        :type address: str()
        :param mailbox_size: Maximum size of the mailbox
        :type mailbox_size: int()
        :param inbox: The inbox queue.  Must have get and put
        :type inbox: Queue()
        :param routing_logic: The logic to use in routing
        :tpye routing_logic: str()
        """
        super().__init__(name, loop, address, mailbox_size, inbox)
        self.register_handler(Pull, self.__pull)
        self.register_handler(PullQuery, self.__pull)
        self.register_handler(Subscribe, self.__subscribe)
        self.register_handler(DeSubscribe, self.__de_subscribe)
        self.subscribers = subscribers
        self.current_index = 0
        self.routing_logic = routing_logic
        self.__tasks = tasks

    def __subscribe(self, message):
        """
        Subscribe to the Source

        :param message: The actor message
        :type message: Message()
        """
        if isinstance(message, Subscribe):
            actor = message.payload
            if actor not in self.subscribers:
                self.subscribers.append(actor)

    def __de_subscribe(self, message):
        """
        De-subscribe from the source

        :param message: The message for the Source
        :type message: str()
        """
        if isinstance(message, DeSubscribe):
            actor = message.payload
            if actor in self.subscribers:
                self.subscribers.remove(actor)
                if self.current_index > 0:
                    if len(self.subscribers) < self.current_index:
                        self.current_index = 0

    async def __pull(self, message):
        """
        Do not overwrite.  Handles on_pull
        """
        try:
            if isinstance(message, Pull):
                sender = message.sender
                result = self.on_pull()
                if result and sender is not None:
                    out_message = Publish(Push(result, self), self)
                    try:
                        await self.tell(sender, out_message)
                    except Exception: 
                        self.handle_fail()
        except Exception:
            self.handle_fail()

    @abstractmethod
    def on_pull(self):
        """
        Implemented by user.  Handles the pull functionality.
        """
        err_msg = "Source Pull Function Not Implemented"
        logging.error(err_msg)
        return None

    @abstractmethod
    def on_complete(self):
        """
        Handle closure of the stages. Submit poison pills.
        """
        logging.error("On Complete Not Overridden")

    def start(self):
        """
        Potentially override the start method.
        """
        super().start()
