'''
A Stage handling input from the source.
Created on Sep 21, 2017

@author: aevans
'''


from compaktor.actor.base_actor import AbstractActor, BaseActor
from compaktor.message.message_objects import PoisonPill, QueryMessage, Pull,\
    Subscribe, SetAccountant, DeSubscribe, FlowResult, Broadcast, RegisterTime
from compaktor.errors.actor_errors import HandlerNotFoundError,\
    FunctionNotProvidedException
from compaktor.streams.objects.base_stage import BaseStage
from compaktor.streams.objects.source import Source
from compaktor.streams.objects.accountant import AccountantActor
import functools
from compaktor.actor.pub_sub import PubSub
from datetime import datetime


class Sink(AbstractActor):
    """
    The source emits vectors.
    """

    def __init__(self, func, name=None, loop=None, address=None,
                 mailbox_size=10000, inbox=None, accountants=[],
                 publisher=PubSub()):
        """
        Constructor

        :param name: The source name
        :type name: str()
        :param loop: The AbstractEventLoop handling i/o and tasks
        :type loop: AbstractEventLoop()
        :param address: The address of the actor
        :type address: str()
        :param mailbox_size: The maximum mailbox size
        :type maibox_size: int()
        :param inbox: The mailbox queue
        :type inbox: asyncio.Queue()
        :param accountants: The 
        """
        super().__init__(name, loop, address)
        if func is None:
            msg = "Function Must be Provided to The Stage"
            raise FunctionNotProvidedException(msg)
        self.__func = func
        self.__max_inbox_size = mailbox_size
        self.__inbox = inbox
        if self.__inbox is None:
            self.__inbox = asyncio.Queue(
                    maxsize=mailbox_size, loop=loop
                )
        self.__publisher = publisher
        self.__accountant = accountants
        self._handlers = {}
        self.__register_handler(FlowResult, self.__handle_push)
        self.__register_handler(PoisonPill, self._stop_message_handler)
        self.__register_handler(Subscribe, self.__subscribe)
        self.__register_handler(DeSubscribe, self.__desubscribe)
        self.__register_handler(SetAccountant, self.__set_accountant)

    async def __handle_push(self, message):
        try:
            f_time = datetime.datetime.now()
            payload = message.payload
            result = self.__func(message)
            await self.publish(FlowResult(result))
            if self.__accountants and len(self.__accountants) > 0:
                f_time = datetime.datetime.now() - f_time
                f_time = f_time.total_seconds()
                diff = datetime.datetime.now() - self.__last_accounting_time
                diff = diff.total_seconds()
                if diff > self.__heartbeat:
                    for acct in self.__accountants:
                        try:
                            await self.tell(acct, RegisterTime(f_time))
                        except Exception as e:
                            self.handle_fail()
        except Exception as e:
            self.handle_fail()
    
    async def __desubscribe(self, message):
        try:
            if message:
                actor = message.payload
                if actor:
                    if isinstance(actor, BaseActor) or\
                        isinstance(actor, BaseStage):
                        msg = DeSubscribe(actor)
                        await self.tell(self.__publisher, msg)
                    else:
                        print("Can only subscribe a stage or actor")
                else:
                    print("Actor or stage must be provided to subscribe")
            else:
                print("Message to subscribe cannot be None")
        except Exception as e:
            self.handle_fail()

    async def __subscribe(self, message):
        try:
            if message:
                actor = message.payload
                if actor:
                    if isinstance(actor, BaseActor) or\
                        isinstance(actor, BaseStage):
                        msg = Subscribe(actor)
                        await self.tell(self.__publisher, msg)
                    else:
                        print("Can only subscribe a stage or actor")
                else:
                    print("Actor or stage must be provided to subscribe")
            else:
                print("Message to subscribe cannot be None")
        except Exception as e:
            self.handle_fail()

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
