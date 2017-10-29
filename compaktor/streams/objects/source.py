'''
Source for obtaining information
Created on Oct 7, 2017

@author: aevans
'''

import asyncio
from compaktor.message.message_objects import Pull, PullQuery, Push
from compaktor.actor.base_actor import BaseActor


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
        self.register_sink_handlers()

    def register_sink_handlers(self):
        self.register_handler(PullQuery, self.__pull)
        self.register_handler(Pull, self.__pull)

    async def on_pull(self, payload):
        """
        Handle payload for pull. Overwrite

        :param payload: Any payload from the pull
        :type payload: object
        """
        return None

    async def __pull(self, message):
        """
        Perform a pull request
        """
        result = []
        try:
            num_els = message.payload
            run = True
            i = 0
            while i < num_els and run:
                val = await self.on_pull(message.payload)
                if val:
                    result.append(val)
                else:
                    run = False
                i += 1
            psh = Push(result, self)
            await self.tell(message.sender, psh)
        except Exception:
            self.handle_fail()
