'''
A sink that writes to a file
Created on Oct 7, 2017

@author: aevans
'''

import asyncio
from compaktor.streams.objects import Sink
from compaktor.actor.pub_sub import PubSub
from compaktor.message.message_objects import Publish

class FileSink(Sink):
    """
    File sink for writing out to a file.
    """

    def __init__(self, name, fdir, fname, loop=asyncio.get_event_loop(),
                 address=None, mailbox_size=1000, inbox=None,
                 accountants=[], publisher=PubSub(), max_part_size=5000):
        super().__init__(self.write_func, name, loop, address, mailbox_size,
                         inbox, accountants, publisher)
        self.file = None
        self.__fname = fname
        self.__fdir = fdir

    def write_func(self, message):
        if isinstance(message, Publish) is False:
            err_msg = "Message is not of Type FlowResult in Sink"
            raise ValueError(err_msg)
