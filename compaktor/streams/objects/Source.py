'''
Source for obtaining information
Created on Oct 7, 2017

@author: aevans
'''

import asyncio
import logging
from compaktor.actor.pub_sub import PubSub
from compaktor.message.message_objects import Pull, Subscribe, DeSubscribe,\
    FlowResult


class Source(PubSub):

    def __init__(self, name, loop=asyncio.get_event_loop(), address=None,
                 mailbox_size=1000, inbox=None, routing_logic="round_robin"):
        super().__init__(name, loop, address, mailbox_size, inbox)
        self.register_handler(Pull, self.pull)
        self.register_handler(Subscribe, self.subscribe)
        self.register_handler(DeSubscribe, self.de_subscribe)
        self.subscribers = []
        self.current_index = 0
        self.routing_logic = routing_logic

    def subscribe(self, message):
        if isinstance(message, Subscribe):
            actor = message.payload
            if actor not in self.subscribers:
                self.subscribers.append(actor)

    def de_subscribe(self, message):
        if isinstance(message, DeSubscribe):
            actor = message.payload
            if actor in self.subscribers:
                self.subscribers.remove(actor)
                if self.current_index > 0:
                    if len(self.subscribers) < self.current_index:
                        self.current_index = 0

    async def pull(self):
        result = self.on_pull()
        if result and len(self.subscribers) > 0:
            message = FlowResult(result)
            try:
                if self.routing_logic.lower().strip() == "round_robin":
                    sub = self.subscribers[self.current_index]
                    asyncio.run_coroutine_threadsafe(self.tell(sub, message))
                    self.current_index += 1
                    if self.current_index is len(self.subscribers):
                        self.current_index = 0
                elif self.routing_logic.lower().strip() == "broadcast":
                    for sub in self.subscribers:
                        asyncio.run_coroutine_threadsafe(self.tell(sub, message))
                else:
                    err_msg = "Source only allows round_robin or braodcast routing logic"
                    logging.error(err_msg)
            except Exception as e: 
                self.handle_fail()

    def on_pull(self):
        err_msg = "Source Pull Function Not Implemented"
        logging.error(err_msg)
        return None

    def on_complete(self):
        """
        Handle closure of the stages. Submit poison pills.
        """
        logging.error("On Complete Not Overridden")
