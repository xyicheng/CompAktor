'''
Date parser stage

Created on Oct 11, 2017

@author: aevans
'''

import asyncio
from compaktor.streams.objects.node_pub_sub import NodePubSub

class DateParserStage(NodePubSub):

    def __init__(self, name, providers, loop=asyncio.get_event_loop(),
                 address=None, mailbox_size=1000, inbox=None,
                 empty_demand_logic="broadcast"()):
        super().__init__(name, providers, loop=loop, address=address,
                         mailbox_size=mailbox_size, inbox=inbox,
                         empty_demand_logic=empty_demand_logic)

    @NodePubSub.abstractmethod
    def on_pull(self, message):
        pass
