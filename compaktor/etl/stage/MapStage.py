'''
A stage for mapping values.
Created on Oct 10, 2017

@author: aevans
'''

import asyncio
from compaktor.streams.objects.NodePubSub import NodePubSub


class MapStage(NodePubSub):

    def __init__(self, name, providers, dict_mappings, reg_mappings=None,
                 loop=asyncio.get_event_loop(), address=None,
                 mailbox_size=1000, inbox=None,
                 empty_demand_logic = "broadcast"):
        super().__init__(name, providers, loop, address, mailbox_size, inbox,
                         empty_demand_logic)
        self.__mappings = dict_mappings
        self.__reg_mappings = reg_mappings

    def on_pull(self, message):
        pass
