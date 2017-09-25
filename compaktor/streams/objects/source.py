'''
Stream source
Created on Sep 21, 2017

@author: aevans
'''


from compaktor.actor.base_actor import AbstractActor, BaseActor
from compaktor.message.message_objects import PoisonPill, QueryMessage, Pull,\
    FlowResult, Subscribe, DeSubscribe, RegisterTime
from compaktor.errors.actor_errors import HandlerNotFoundError
from compaktor.streams.objects.base_stage import BaseStage
from compaktor.actor.pub_sub import PubSub
import datetime
from datetime import date


class Source(BaseStage):
    """
    The source emits objects to be handled within the stream
    """

    def __init__(self, pull_func, name=None, loop=None, address=None,
                 mailbox_size=10000, inbox=None, pub=PubSub(), accountants=[],
                 heartbeat=15):
        """
        Constructor

        :param name: The source name
        :type name: str()
        :param loop: The AbstractEventLoop handling i/o and tasks
        :type loop: AbstractEventLoop()
        :param address: The address of the actor
        :type address: str()
        """
        super().__init__(name, loop, address, mailbox_size, inbox, pub)
        self.__pull_func = pull_func
        self.__heartbeat=heartbeat
        self.__last_accounting_time=datetime.datetime.now()
        self.__accountants=accountants
        self.register_handler(Pull, self.__handle_pull)
        self.register_handler(Subscribe, self.__subscribe)

    async def __handle_pull(self, message):
        """
        Handle a pull request.  Also sends to the accountant if the time is
        appropriate.
        """
        try:
            f_time = datetime.datetime.now()
            payload = message.payload
            result = self.__pull_func(payload)
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

    async def __subscribe(self, message):
        try:
            if message and isinstance(message, Subscribe):
                actor = message.payload
                if actor and isinstance(actor, BaseActor) or\
                    isinstance(actor, BaseStage):
                    await self.tell(self.__publisher, message)
                else:
                    print("Subscribe must contain an Actor or Stage")
            else:
                msg = "Only Subscribe messages with an actor may be passed "
                msg += "to __subscribe"
                print(msg)
        except Exception as e:
            self.handle_fail()

    async def __desubscribe(self, message):
        try:
            if message and isinstance(message, DeSubscribe):
                actor = message.payload
                if actor and isinstance(actor, BaseActor) or\
                    isinstance(actor, BaseStage):
                    await self.tell(self.__publisher, message)
                else:
                    print("Subscribe must contain an Actor or Stage")
            else:
                msg = "Only Subscribe messages with an actor may be passed "
                msg += "to __subscribe"
                print(msg)
        except Exception as e:
            self.handle_fail()
