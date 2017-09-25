'''
An accountant actor for keeping track of tics.

Created on Sep 23, 2017

@author: aevans
'''


import asyncio
import datetime
import math
from compaktor.actor.abstract_actor import AbstractActor
from compaktor.message.message_objects import Subscribe, RegisterTime,\
    PoisonPill, QueryMessage
from compaktor.actor.base_actor import BaseActor
from compaktor.errors.actor_errors import HandlerNotFoundError
from datetime import date
from compaktor.state.actor_state import ActorState


class AccountantActor(AbstractActor):

    def __init__(self, tell_heartbeat=30, min_heartbeat=.05, name=None,
                 loop=asyncio.get_event_loop(), address=None):
        super().__init__(name, loop, address)
        self.__sources = []
        self.__avgs = {}
        self.__min_heartbeat = min_heartbeat 
        self.__tell_heartbeat = tell_heartbeat
        self.__last_tell_time = datetime.datetime.now()
        self.__register_handler(Subscribe, self.__register_actor)
        self.__register_handler(RegisterTime, self.__register_time)

    async def __register_actor(self, message):
        try:
            if message:
                actor = message.payload
                if actor and isinstance(actor, AbstractActor):
                    if actor not in self.__sources:
                        self.__sources.append(actor)
                else:
                    print("Accountant can only Register Abstract ACtors")
            else:
                print("Accountant can only take a message")
        except Exception as e:
            self.handle_fail()

    async def __register_time(self, message):
        try:
            if message:
                t_dict = message.payload
                if t_dict and isinstance(t_dict, dict):
                    for tup in t_dict.items():
                        k = tup[0]
                        v = tup[1]
                        if k in t_dict.keys():
                            if len(self.__avgs[k]) > 3:
                                self.__avgs[k].pop()
                            self.__avgs[k].append(v)
                        else:
                            self.__avgs[k]=v
            current_time = datetime.datetime.now()
            diff = self.__last_tell_time - current_time
            if diff.total_seconds() >= self.__tell_heartbeat:
                max_time = self.__min_heartbeat
                for tup in self.__avgs.items():
                    avg = sum(tup[1]) / 3
                    max_time = max([avg, max_time])

                rem = []
                for a in self.__sources:
                    if isinstance(a, BaseActor):
                        if a.get_state() is ActorState.RUNNING:
                            await self.tell(a, RegisterTime(max_time))
                        else:
                            rem.append(a)
                if len(rem) > 0:
                    (self.__sources.remove(a) for a in rem)

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
