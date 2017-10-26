'''
Source for obtaining information
Created on Oct 7, 2017

@author: aevans
'''

import asyncio
from compaktor.message.message_objects import Pull, PullQuery, Publish, Push
from compaktor.actor.base_actor import BaseActor
import logging


class Source(BaseActor):
    """
    Source.  Can be chained to provide information.  Demand typically comes in
    via round robin logic.  Nodes subscribe to the source.  The user implements
    the on_pull method in their extended Source.
    """

    def __init__(self, name,loop=asyncio.get_event_loop(), address=None,
                 mailbox_size=1000, inbox=None):
        """
        Constructor

        :param name: The name of the source. Should be uniques.
        :type name: str()
        :param loop: The loop for the Source
        :type loop: AbstractEventLoop()
        :param address: The unique address for the Source
        :type address: str()
        :param mailbox_size: Maximum size of the mailbox
        :type mailbox_size: int()
        :param inbox: The inbox queue.  Must have get and put
        :type inbox: Queue()
        """
        super().__init__(name, loop, address, mailbox_size, inbox)
        self.register_handler(PullQuery, self.__pull)
        self.register_handler(Pull, self.__pull)

    async def on_pull(self, payload):
        """
        Implement the this in a separate function.

        :param payload: The payload from the message
        :type payload: object
        """
        pass

    async def __pull(self, message):
        try:
            if message and message.sender:
                sender = message.sender
                load = message.payload
                result = await self.on_pull(load)
                out_message = Push(result, sender)
                await self.tell(sender, out_message)
            else:
                logging.error("Cannot")
        except Exception:
            self.handle_fail()
