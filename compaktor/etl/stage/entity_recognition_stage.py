'''
Entity Recognition

Created on Oct 11, 2017

@author: aevans
'''

import asyncio
from compaktor.streams.objects.NodePubSub import NodePubSub
from compaktor.etl.objects.row import RowVector

class EntityRecognition(NodePubSub):

    def __init__(self, name, providers, loop=asyncio.get_event_loop(),
                 address=None, mailbox_size=1000, inbox=None,
                 empty_demand_logic = "broadcast"()):
        super().__init__(self, name, providers, loop=asyncio.get_event_loop(),
                 address=None, mailbox_size=1000, inbox=None,
                 empty_demand_logic = "broadcast"())

    @NodePubSub.abstractmethod
    def on_pull(self, message):
        try:
            payload = message.payload
            if isinstance(payload, RowVector):
                pass
        except Exception as e:
            self.handle_fail()
