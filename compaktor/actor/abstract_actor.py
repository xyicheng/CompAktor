'''
Created on Sep 22, 2017

@author: aevans
'''

import asyncio
import time
import traceback
from compaktor.utils.name_utils import NameCreationUtils
from compaktor.registry import actor_registry as registry
from compaktor.state.actor_state import ActorState
from compaktor.message.message_objects import QueryMessage
import pdb
from nose.plugins.debug import Pdb


class AbstractActor(object):
    """
    The basis for all actors is the AbstractActor
    """

    def __init__(self, name=None, loop=None, address=None):
        """
        Constructor

        :param name: Actor name
        :type name: str()
        :param loop: Asyncio event loop
        :type loop: asyncio.events.AbstractEventLoop()
        :param address: Address for the actor
        :type address: str()
        """
        self.__STATE = ActorState.CREATED
        self.loop = loop
        if self.loop is None:
            self.loop = asyncio.get_event_loop()

        self.name = name
        if name is None:
            self.name = str(NameCreationUtils.get_name_base())
        self.__STATE = ActorState.LIMBO
        self.__complete = asyncio.Future(loop=self.loop)
        self.__address = address

    def get_name(self):
        """
        Get the actor name
        :return: The actor name
        :rtype: str()
        """
        return self.name

    def get_state(self):
        """
        Get the current actor state

        :return:  The actor state
        :rtype: ActorState.value()
        """
        return self.__STATE

    def pre_start(self):
        """
        A method to run before the actor is started.
        """
        print("Starting Actor {}".format(time.time()))
        self.__STATE = ActorState.CREATED

    def start(self):
        """
        Start the actor
        """
        self.pre_start()
        self.__STATE = ActorState.RUNNING
        self.loop.create_task(self._run())

    def _start(self):
        """
        Do not touch
        """
        pass

    async def _run(self):
        """
        Complete tasks while the state is running
        """
        while self.__STATE == ActorState.RUNNING:
            await self._task()
        self.__complete.set_result(True)

    async def stop(self):
        """
        Performs actual stop logic.

        :return: A boolean on completion
        :rtype: bool()
        """
        self.__STATE = ActorState.STOPPED
        await self._stop()
        await self.__complete
        self.post_stop()
        return True

    def start_to_stop(self):
        """
        Execute the stop command, returning the future to be awaited on.
        The state is set to LIMBO.  It should be set to STOPPED by the
        programmer.  Typical logging messages are not produced either.

        :return: The awaitable future
        :rtype: asyncio.Future()
        """
        self.__STATE = ActorState.LIMBO
        return asyncio.ensure_future(self.stop())

    async def _stop(self):
        """
        Called at beginning of stop command.
        """
        print("Stopping Actor {}".format(time.time()))

    def post_stop(self):
        """
        Logs a stopped message
        """
        print("Actor Stopped {}".format(time.time()))
        self.__STATE = ActorState.TERMINATED

    def post_restart(self):
        """
        Called after an actor is restarted.
        """
        print("Actor Restarted {}".format(time.time()))

    async def _task(self):
        """
        An overriden method for implementing a function
        """
        raise NotImplementedError(
            "The method receive Not Yet Implemented in Actor.")

    async def tell(self, target, message):
        """
        Submit a message to a target actor without blocking.

        :param target:  The target actor
        :type target:  AbtractActor
        :param message: The appropriate message to send
        :type message:  Message()
        """
        try:
            if isinstance(target, str):
                target = registry.get_registry().find_node(target)
            if target:
                await target._receive(message)
            else:
                print("Target Does Not Exist")
        except AttributeError as ex:
            err = "Target Does not Have a _receive method. Is it an actor?"
            raise TypeError(err) from ex

    async def ask(self, target, message):
        """
        Submit a message to a target actor and wait for a response

        :param target:  The target actor
        :type target:  AbstractActor
        :param message:  The appropriate message to send
        :type message:  QueryMessage
        """
        try:
            assert isinstance(message, QueryMessage)
            if not message.result:
                message.result = asyncio.Future(loop=self.loop)
            asyncio.run_coroutine_threadsafe(self.tell(target, message),self.loop)
            res = await message.result
            return res
        except Exception as e:
            self.handle_fail()

    def handle_fail(self):
        """
        Handle a failure. The default just logs the stack trace.
        """
        traceback.print_exc()
