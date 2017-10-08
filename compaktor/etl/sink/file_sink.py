'''
A sink that writes to a file
Created on Oct 7, 2017

@author: aevans
'''

import asyncio
from compaktor.streams.objects.sink import Sink
from compaktor.actor.pub_sub import PubSub
from compaktor.message.message_objects import FlowResult
from ctypes.test.test_array_in_pointer import Value

class FileSink(Sink):

    def __init__(self, name, fpath, loop=asyncio.get_event_loop(),
                 address=None, mailbox_size=1000, inbox=None,
                 accountants=[], publisher=PubSub()):
        super().__init__(self.write_func, name, loop, address, mailbox_size,
                         inbox, accountants, publisher)
        self.file = None

    def write_func(self, message):
        if isinstance(message, FlowResult) is False:
            err_msg = "Message is not of Type FlowResult in Sink"
            raise ValueError(err_msg)
        pass
