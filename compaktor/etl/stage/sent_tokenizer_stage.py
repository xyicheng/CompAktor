'''
Created on Oct 9, 2017

@author: aevans
'''

import asyncio
import nltk
from compaktor.streams.objects.NodePubSub import NodePubSub

class SentTokenizerStage(NodePubSub):

    def __init__(self, name, providers, loop=asyncio.get_event_loop(),
                 address=None, mailbox_size=1000, inbox=None,
                 empty_demand_logic = "broadcast"):
        super().__init__(name, providers, loop, address, mailbox_size, inbox,
                         empty_demand_logic)

    def on_pull(self, message):
        try:
            payload = message.payload
            sents = []
            if isinstance(payload, str):
                sents = nltk.sent_tokenize(payload)
            return sents
        except Exception as e:
            self.handle_fail()
