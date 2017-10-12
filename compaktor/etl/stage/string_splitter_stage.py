'''
Split Strings
Created on Oct 9, 2017

@author: aevans
'''

import asyncio
import re
from compaktor.streams.objects.NodePubSub import NodePubSub

class StringSplitterStage(NodePubSub):

    def __init__(self, name, split_regex, providers,
                 loop=asyncio.get_event_loop(), address=None,
                 mailbox_size=1000, inbox=None,
                 empty_demand_logic = "broadcast"):
        super().__init__(name, providers, loop, address, mailbox_size, inbox,
                         empty_demand_logic)
        self.__split_regex = split_regex

    def on_pull(self, message):
        try:
            payload = message.payload
            if isinstance(payload, str):
                return re.split(self.__split_regex)
        except Exception as e:
            self.handle_fail()