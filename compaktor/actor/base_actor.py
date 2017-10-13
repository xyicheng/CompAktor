'''
Basic actor classes.

Created on Aug 18, 2017

@author: aevans
'''

import asyncio
import logging
from compaktor.actor.abstract_actor import AbstractActor
from compaktor.errors.actor_errors import HandlerNotFoundError
from compaktor.message.message_objects import QueryMessage, PoisonPill
from compaktor.utils.name_utils import NameCreationUtils 
from abc import abstractmethod


class BaseActor(AbstractActor):
    """
    The base actor implementing the AbstractActor class.
    """

    def __init__(self,  name=None, loop=None, address=None, mailbox_size=10000,
                 inbox=None):
        """
        Constructor

        :param name: The actor name
        :type name: str()
        :param loop: The actors AbstractEventLoop
        :type loop: AbstractEventLoop()
        :param address: The unique address for the actor
        :type address: str()
        :param mailbox_size: The max size of the inbox
        :type mailbox_size: int()
        :param inbox: The inbox queue
        :type inbox: asyncio.Queue()
        """
        if name is None:
            str(NameCreationUtils.get_name_base())
        self.__address = address
        super().__init__(name, loop, address)
        self.__max_inbox_size = mailbox_size
        self.__inbox = inbox
        if self.__inbox is None:
            self.__inbox = asyncio.Queue(
                maxsize=self.__max_inbox_size, loop=self.loop)
        self._handlers = {}
        self.register_handler(PoisonPill, self._stop_message_handler)

    def get_name(self):
        """
        Get the actor name

        :return: The name of the actor
        :rtype:  str()
        """
        return self.name

    def register_handler(self, message_cls, func):
        """
        Registers a function for to be run when a type of message is used.
        Both a Message and QueryMessage may be supplied

        :param message:  Thee message class
        :type: <class Message>
        :param func: The function to register.
        :type func: def
        """
        self._handlers[message_cls] = func
    
    @abstractmethod    
    async def _task(self):
        """
        The running task.  It is not recommended to override this function.
        """
        message = await self.__inbox.get()
        try:
            handler = self._handlers[type(message)]
            is_query = isinstance(message, QueryMessage)
            try:
                if handler:
                    response = await handler(message)
                else:
                    logging.warning("Handler is NoneType")
                    self.handle_fail()
            except Exception as ex:
                if is_query:
                    message.result.set_exception(ex)
                else:
                    logging.warning('Unhandled exception from handler of '
                                    '{0}'.format(type(message)))
                    self.handle_fail()
            else:
                if is_query:
                    message.result.set_result(response)
        except KeyError as ex:
            self.handle_fail()
            raise HandlerNotFoundError(type(message)) from ex

    async def _stop(self):
        """
        Waits for a poison pill.
        """
        await self._receive(PoisonPill())

    async def _receive(self, message):
        """
        Receive function for handling inbound messages
        The message may be of type Message or QueryMessage

        :param message:  The message to enqueue
        :type message:  Message()
        """
        await self.__inbox.put(message)

    async def _stop_message_handler(self, message):
        '''The stop message is only to ensure that the queue has at least one
        item in it so the call to _inbox.get() doesn't block. We don't actually
        have to do anything with it.
        '''

    def __str__(self, *args, **kwargs):
        """
        Get the actors string representation.

        :param args: List arguments
        :type args: list()
        :param kwargs: supplied kwargs
        :type kwargs: dict()

        """
        return "Actor(name = {}, handlers = {}, status = {})".format(
            self.name, str(self._handlers), self.get_state())

    def __repr__(self, *args, **kwargs):
        """
        Get the represenation from the AbstractoActor

        :param args: list args
        :type args: list()
        :param kwargs: Dictionary arguments
        :type kwargs: dict()
        """
        return AbstractActor.__repr__(self, *args, **kwargs)
