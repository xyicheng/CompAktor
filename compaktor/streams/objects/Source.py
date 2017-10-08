'''
Source for obtaining information
Created on Oct 7, 2017

@author: aevans
'''

import asyncio
import logging
from compaktor.actor.pub_sub import PubSub
from compaktor.message.message_objects import Pull


class Source(PubSub):

    def __init__(self, name, loop=asyncio.get_event_loop(), address=None,
                 mailbox_size=1000, inbox=None):
        super().__init__(name, loop, address, mailbox_size, inbox)
        self.register_handler(Pull, self.pull)

    def pull(self):
        err_msg = "Source Pull Function Not Implemented"
        logging.error(err_msg)

    def on_complete(self):
        """
        Handle closure of the stages. Submit poison pills.
        """
        logging.error("On Complete Not Overridden")
