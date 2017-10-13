'''
Created on Oct 12, 2017

@author: aevans
'''

import asyncio
from compaktor.actor.base_actor import BaseActor
from compaktor.message.message_objects import Message, QueryMessage


class StringMessage(Message):
    pass


class IntMessage(Message):
    pass


class AddIntMessage(QueryMessage):
    pass


class ObjectMessage(Message):
    pass


class ObjectTestActor(BaseActor):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.register_handler(ObjectMessage, self.print_status)

    def print_status(self, message):
        print("Received Payload")
        print(message.__repr__())


class StringTestActor(BaseActor):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.register_handler(StringMessage, self.print_status)

    def print_status(self, message):
        print(message.payload)


class AddTestActor(BaseActor):

    def __init__(self, name, loop=asyncio.get_event_loop(),
                 address=None, mailbox_size=1000, inbox=None):
        super().__init__(name, loop, address, mailbox_size, inbox)
        self.register_handler(QueryMessage, self.add_test)
        self.register_handler(AddIntMessage, self.add_test)

    async def add_test(self, message):
        print("Received")
        return message.payload + 1

