'''
Generic Sink
Created on Oct 7, 2017

@author: aevans
'''

import asyncio
import logging
from compaktor.actor.pub_sub import PubSub
from compaktor.message.message_objects import Pull, Publish


class Sink(PubSub):
    """
    Sink which should be the final stage in any stream.  Sinks are the o
    in io as opposed to sources which are the i.  The user implements the
    on_push.
    """

    def __init__(self, name, loop=asyncio.get_event_loop(), address=None,
                 mailbox_size=1000, inbox=None):
        """
        Sink Constructor.

        :param name: The name of the sink
        :type name: str()
        :param loop: The loop to run the sink on
        :type loop: AbstractEventLoop()
        :param address: Address for the actors
        :type address: str()
        :param mailbox_size: Maximum size of the mailbox
        :type mailbox_size: int()
        :param inbox: Mailbox queue to use must have get() and put() methods
        :type inbox: Queue()
        """
        super().__init__(name, loop, address, mailbox_size, inbox)
        self.register_handler(Publish, self.push)

    def push(self, message):
        """
        Standard push function.

        :param message: The message to push
        :type message: Message()
        """
        try:
            self.on_push(message)
        except Exception as e:
            self.handle_fail()

    def on_push(self, message):
        """
        Custom push function

        :param message: The message to use
        :type message: Message()
        """
        err_msg = "Source Pull Function Not Implemented"
        logging.error(err_msg)

    def on_complete(self):
        """
        Handle closure of the stages. Submit poison pills.
        """
        logging.error("On Complete Not Overridden")