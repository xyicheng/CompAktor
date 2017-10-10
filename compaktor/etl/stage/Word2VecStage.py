'''
Word vectorizer using the actor system
Created on Oct 10, 2017

@author: aevans
'''

import asyncio
import nltk
from compaktor.streams.objects.NodePubSub import NodePubSub

class Word2VecStage(NodePubSub):

    def __init__(self, name, providers,
                 loop=asyncio.get_event_loop(), address=None,
                 mailbox_size=1000, inbox=None,
                 empty_demand_logic = "broadcast"):
        super().__init__(name, providers, loop, address, mailbox_size, inbox,
                         empty_demand_logic)

    def on_pull(self, message):
        try:
            payload = message.payload
            word_dict = {}
            if payload and isinstance(payload, str):
                words = nltk.word_tokenize(payload)
                if words:
                    words = [word.lower().strip() for word in words]
                    for word in set(words):
                        ct = words.count(word)
                        word_dict[word] = ct
                return word_dict
            else:
                return word_dict
        except Exception as e:
            self.handle_fail()
