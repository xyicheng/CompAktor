'''
Created on Oct 12, 2017

@author: aevans
'''

import asyncio
from compaktor.actor.base_actor import BaseActor
from compaktor.message.message_objects import Message, TaskMessage, Push


class TaskActor(BaseActor):
    """
    Actor for the Stage Tasks.  Expects the user to supply an on_call
    function.
    """

    def __init__(self,name, on_call, loop=asyncio.get_event_loop(),
                 address=None, mailbox_size=1000, inbox=None):
        """
        Constructor

        :param name: The name of the actor
        :type name: str()
        :param on_call: The on_call function executed on message receipt
        :type on_call: async function
        :param loop: The loop to use
        :type loop: AbstractEventLoop
        :param address: Address of the actor
        :type address: str()
        :param mailbox_size: The max size of the mailbox
        :type mailbox_size: int()
        :param inbox: The inbox to use
        :type inbox: Queue()
        """
        super().__init__(name, loop, address, mailbox_size, inbox)
        self.register_handler(TaskMessage, self.__call_back)
        self.__on_call = on_call

    async def __call_back(self, message):
        """
        Perfroms the on call and returns the result by pushing back
        to the sender.

        :param message: The Message for the callback with the sender
        :type message: Message()
        """
        try:
            if isinstance(message, Message):
                sender = message.sender
                caller = message.caller
                if sender is None:
                    raise ValueError("Sender is None")
                result = await self.__on_call(message.payload)
                await self.tell(sender, Push(result, caller))
        except Exception as e:
            self.handle_fail()
