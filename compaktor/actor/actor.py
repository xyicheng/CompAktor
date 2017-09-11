'''
Basic actor classes.

Created on Aug 18, 2017

@author: aevans
'''


import asyncio
from enum import Enum
import logging
import time
import traceback
from atomos.atomic import AtomicLong
from compaktor.actor.message import QueryMessage, PoisonPill


#long will be fine for now
class NameCreationUtils():
    """
    Utilities for getting the base name of the class
    """
    NAME_BASE = AtomicLong()

    @staticmethod
    def get_name_base():
        base = NameCreationUtils.NAME_BASE.get_and_add(1)
        if NameCreationUtils.NAME_BASE.get() is float('inf'):
            NameCreationUtils.NAME_BASE.set(0)
        return base


class HandlerNotFoundError(Exception): pass


class ActorState(Enum):
    """
    The state of the actors.  The following states exist.
    
    - STOPPED:  The actor is stopped after running but post_stop was not called
    - RUNNING:  The actor is running
    - LIMBO:  The actor is starting but not yet started
    - TERMINATED:  The actor has been terminated and post_stop called
    - CREATED:  The actor is created but not started
    """
    STOPPED = 0
    RUNNING = 1
    LIMBO = 2
    TERMINATED = 3
    CREATED = 4
    

class AbstractActor(object):
    """
    The basis for all actors is the AbstractActor
    """
    
    __STATE = ActorState.CREATED
    __NAME = None  
    
    def __init__(self, name = None, loop = None, address = None):
        """
        Constructor
        
        :param name: Actor name
        :type name: str()
        :param loop: Asyncio event loop
        :type loop: asyncio.events.AbstractEventLoop()
        :param address: Address for the actor
        :type address: str()
        """
        self.loop = loop
        if self.loop is None:
            self.loop = asyncio.get_event_loop()
        
        self.__NAME = name
        if name is None:
            self.__NAME = str(NameCreationUtils.get_name_base())
        self.__STATE = ActorState.LIMBO
        self.__complete = asyncio.Future(loop = self.loop)
        self.__address = address
    
    def get_name(self):
        """
        Get the actor name
        :return: The actor name
        :rtype: str()
        """
        return self.__NAME

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
        logging.debug("Starting Actor {}".format(time.time()))
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
        logging.log("Stopping Actor {}".format(time.time()))
     
    def post_stop(self):
        """
        Logs a stopped message
        """
        logging.debug("Actor Stopped {}".format(time.time()))
        self.__STATE = ActorState.TERMINATED

    def post_restart(self):
        """
        Called after an actor is restarted.
        """
        logging.debug("Actor Restarted {}".format(time.time()))
        
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
            await target._receive(message) 
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
        assert isinstance(message, QueryMessage)
        if not message.result:
            message.result = asyncio.Future(loop = self.loop)
        await self.tell(target, message)
        res = await message.result
        return res 

    def handle_fail(self):
        """
        Handle a failure. The default just logs the stack trace.
        """
        logging.error(traceback.format_exc())


class BaseActor(AbstractActor):
    """
    The base actor implementing the AbstractActor class.
    """
    
    __NAME = None
    
    def __init__(self, *args, **kwargs):
        """
        Constructor
        
        :Keyword Arguments:
            -name (str): The actor name
            -loop (AbstractEventLoop): The actor event loop
            -address (str): An actors address
            -max_inbox_size (int): Max number of messages in the inbox
        """
        name = kwargs.get('name', None)
        loop = kwargs.get('loop', None)
        address = kwargs.get('address', None)
        abs_kwargs = {'name' : name, 'loop' : loop, 'address' : address}
        super().__init__(*args, **abs_kwargs)
        self.__NAME = kwargs.get('name',str(NameCreationUtils.get_name_base()))
        self.loop = kwargs.get('loop', self.loop)
        self._max_inbox_size = kwargs.get('max_inbox_size', 0)
        self._inbox = kwargs.get('queue', asyncio.Queue(maxsize=
                                self._max_inbox_size, loop = self.loop))
        self._handlers = {}

        # Create handler for the 'poison pill' message
        self.register_handler(PoisonPill, self._stop_message_handler)
    
    def get_name(self):
        """
        Get the actor name
        
        :return:  
        """
        return self.__NAME
    
    def register_handler(self, message_cls, func):
        self._handlers[message_cls] = func
    
    async def _task(self):
        message = await self._inbox.get()
        try:
            handler  = self._handlers[type(message)]
            is_query = isinstance(message, QueryMessage)
            try:
                if handler is not None:
                    response = await handler(message)
                else:
                    logging.warning("Handler is NoneType")
            except Exception as ex:
                if is_query:
                    message.result.set_exception(ex)
                else:
                    logging.warning('Unhandled exception from handler of '
                        '{0}'.format(type(message)))
            else:
                if is_query:
                    message.result.set_result(response)
        except KeyError as ex:
            raise HandlerNotFoundError(type(message)) from ex
    
    async def _stop(self):
        await self._receive(PoisonPill())
    
    async def _receive(self, message):
        await self._inbox.put(message)
    
    async def _stop_message_handler(self, message): 
        '''The stop message is only to ensure that the queue has at least one
        item in it so the call to _inbox.get() doesn't block. We don't actually
        have to do anything with it.
        '''
        
    def __str__(self, *args, **kwargs):
        return "Actor(name = {}, handlers = {}, status = {})".format(self.__NAME, str(self._handlers), self.get_state())
    
    def __repr__(self, *args, **kwargs):
        return AbstractActor.__repr__(self, *args, **kwargs) 
