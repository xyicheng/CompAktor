'''
Stream source
Created on Sep 21, 2017

@author: aevans
'''


from compaktor.actor.base_actor import AbstractActor, BaseActor
from compaktor.message.message_objects import PoisonPill, QueryMessage, Pull,\
    Subscribe, SetAccountant, Tick
from compaktor.errors.actor_errors import HandlerNotFoundError
from compaktor.streams.base_stage import BaseStage
from compaktor.streams.source import Source
from compaktor.streams import Accountant
import functools


class TickActor(AbstractActor):
    """
    The source emits vectors.
    """

    def __init__(self, name=None, loop=None, address=None,
                 mailbox_size=10000, inbox=None, accountant=None):
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
        self.__accountant = accountant
        self._handlers = {}
        self.__register_handler(PoisonPill, self._stop_message_handler)
        self.__register_handler(Tick, self.__tick)
        self.__register_handler(Subscribe, self.__subscribe)
        self.__register_handler(SetAccountant, self.__set_accountant)

    async def __set_accountant(self, message):
        """
        Set the accountant in the tick actor
        """
        try:
            if message:
                actor = message.payload
                if isinstance(actor, Accountant):
                    self.__accountant = actor
                else:
                    print("Subscribe must contain Accountant in Tick")
            else:
                print("Accountant cannot be None when set")
        except Exception as e:
            self.handle_fail()

    async def __subscribe(self, message):
        try:
            if message:
                actor = message.payload
                if isinstance(actor, BaseActor) or\
                 isinstance(actor, BaseStage):
                    await self.tell(self.__publisher, actor)
            else:
                print("Subscription message for tic actor was none")
        except Exception as e:
            self.handle_fail()

    async def __tick(self, message):
        try:
            if message:
                if isinstance(message, Pull):
                    await self.tell(self.__publisher, message)
                else:
                    print("Message for tick must be Pull")
            else:
                print("MMessage for tick cannot be None")
        except:
            self.handle_fail()

    async def __handle_tick_iteration(self):
        await self.tick(Tick())
        self.loop.call_later(self._tick_time.get(),functools.partial(self._do_loop))

    def _do_loop(self):
        """
        Starts the tick loop. Called by the constructor
        """
        asyncio.ensure_future(self.__handle_tick_iteration())

    def __register_handler(self, message_cls, func):
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
        Receive function for handling inbound messages
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
