'''
Text Tiling Tokenizig Stage
Created on Oct 9, 2017

@author: aevans
'''

import asyncio
from nltk.tokenize import texttiling
from compaktor.streams.objects.NodePubSub import NodePubSub
from nltk.tokenize.texttiling import TextTilingTokenizer

class TextTilingTokenizerStage(NodePubSub):

    def __init__(self, name, providers, w=20, k=10, loop=asyncio.get_event_loop(),
                 address=None, mailbox_size=1000, inbox=None,
                 empty_demand_logic = "broadcast"):
        super().__init__(name, providers, loop, address, mailbox_size, inbox,
                         empty_demand_logic)
        self.__tiler = TextTilingTokenizer(w=w,k=k)

    def on_pull(self, message):
        try:
            payload = message.payload
            if isinstance(payload, str):
                return self.__tiler.tokenize(payload)
            return None
        except Exception as e:
            self.handle_fail()
