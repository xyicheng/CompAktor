'''
Base Stage
Created on Sep 21, 2017

@author: aevans
'''


from compaktor.actor.base_actor import AbstractActor
from compaktor.message.message_objects import PoisonPill, QueryMessage, Pull,\
    Publish
from compaktor.errors.actor_errors import HandlerNotFoundError
from compaktor.actor.pub_sub import PubSub


class BaseStage(AbstractActor):
    """
    A basic stage actor. Extend this to create new stream objects.
    """

    def __init__(self, name=None, loop=None, address=None,
                 mailbox_size=10000, inbox=None, pub=PubSub()):
        """
        Constructor

        :param name: The source name
        :type name: str()
        :param loop: The AbstractEventLoop handling i/o and tasks
        :type loop: AbstractEventLoop()
        :param address: The address of the actor
        :type address: str()
        """
        super().__init__(name, loop, address)
        self.__max_inbox_size = mailbox_size
        self.__inbox = inbox
        if self.__inbox is None:
            self.__inbox = asyncio.Queue(
                    maxsize=mailbox_size, loop=loop
                )
        self.__publisher = pub

    async def publish(self, message):
        await self.__publisher.tell(self.__publisher, Publish(message))

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
            self.__NAME, str(self._handlers), self.get_state())

    def __repr__(self, *args, **kwargs):
        """
        Get the represenation from the AbstractoActor

        :param args: list args
        :type args: list()
        :param kwargs: Dictionary arguments
        :type kwargs: dict()
        """
        return AbstractActor.__repr__(self, *args, **kwargs)

    async def _receive(self, message):
        """
        Receive functin for handling inbound messages
        The message may be of type Message or QueryMessage

        :param message:  The message to enqueue
        :type message:  Message()
        """
        await self._inbox.put(message)

    async def _stop(self):
        """
        Waits for a poison pill.
        """
        await self._receive(PoisonPill())

    async def _task(self):
        """
        The running task.  It is not recommended to override this function.
        """
        message = await self._inbox.get()
        try:
            handler = self._handlers[type(message)]
            is_query = isinstance(message, QueryMessage)
            try:
                if handler:
                    response = await handler(message)
                else:
                    print("Handler is NoneType")
                    self.handle_fail()
            except Exception as ex:
                if is_query:
                    message.result.set_exception(ex)
                else:
                    print('Unhandled exception from handler of '
                                    '{0}'.format(type(message)))
                    self.handle_fail()
            else:
                if is_query:
                    message.result.set_result(response)
        except KeyError as ex:
            self.handle_fail()
            raise HandlerNotFoundError(type(message)) from ex