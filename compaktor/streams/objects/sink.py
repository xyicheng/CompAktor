'''
Generic Sink
Created on Oct 7, 2017

@author: aevans
'''

import asyncio
import logging
from compaktor.actor.pub_sub import PubSub
from compaktor.message.message_objects import Pull, Publish
from abc import abstractmethod
from multiprocessing import cpu_count
import pdb


class Sink(PubSub):
    """
    Sink which should be the final stage in any stream.  Sinks are the o
    in io as opposed to sources which are the i.  The user implements the
    on_push.
    """

    def __init__(self, name, providers=[], loop=asyncio.get_event_loop(), address=None,
                 mailbox_size=1000, inbox=None, concurrency=cpu_count()):
        """
        Sink Constructor.

        :param name: The name of the sink
        :type name: str()
        :param loop: The loop to run the sink on
        :type loop: AbstractEventLoop()
        :param address: Address for the actors
        :type address: str()
        :param mailbox_size: Maximum size of the mailbox
        :type mailbox_size: int()
        :param inbox: Mailbox queue to use must have get() and put() methods
        :type inbox: Queue()
        """
        super().__init__(name, loop, address, mailbox_size, inbox)
        self.register_handler(Publish, self.__push)
        self.__providers = providers
        self.__concurrency = concurrency

    def start(self):
        super().start()
        if len(self.__providers) > 0:
            for provider in self.__providers:
                asyncio.run_coroutine_threadsafe(
                    self.tell(provider, Pull(None, self)))

    def __do_pull_tick(self):
        """
        Do a pull tick
        """
        try:
            if self._task_q.full() is False:
                sender = self.__providers[self.__current_provider]
                self.loop.run_until_complete(
                    self.tell(self,Pull(None, sender)))
                self.__current_provider += 1
                if self.__current_provider >= len(self.__providers):
                    self.__current_provider = 0
        except Exception as e:
            self.handle_fail()

        try:
            self.loop.call_later(self.tick_delay, self.__do_pull_tick())
        except Exception as e:
            self.handle_fail()

    def __pull_tick(self):
        self.loop.call_later(self.tick_delay, self.__do_pull_tick())

    async def __push(self, message):
        """
        Standard push function.

        :param message: The message to push
        :type message: Message()
        """
        try:
            if isinstance(message, Publish):
                sender = message.sender
                self.on_push(message)
                if sender:
                    await self.tell(sender, Pull(None, self))
        except Exception as e:
            self.handle_fail()

    @abstractmethod
    def on_push(self, message):
        """
        Custom push function

        :param message: The message to use
        :type message: Message()
        """
        err_msg = "Source Pull Function Not Implemented"
        logging.error(err_msg)

    @abstractmethod
    def on_complete(self):
        """
        Handle closure of the stages. Submit poison pills.
        """
        logging.error("On Complete Not Overridden")
