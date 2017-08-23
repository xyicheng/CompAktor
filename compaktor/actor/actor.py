'''
Basic actor classes.

Created on Aug 18, 2017

@author: aevans
'''


import asyncio
import asyncore
from enum import Enum
import logging
import socket
import time
from compaktor.actor.message import QueryMessage, PoisonPill


class HandlerNotFoundError(Exception): pass


class ActorState(Enum):
    STOPPED = 0
    RUNNING = 1
    LIMBO = 2
    TERMINATED = 3
    CREATED = 4
    

class AbstractActor(object):
    
    
    __STATE = ActorState.CREATED
    
    
    def __init__(self,name = None, loop = None, address = None):
        self.loop = asyncio.get_event_loop() if loop is None else loop
        self.__name = name
        self.__STATE = ActorState.LIMBO
        self.__complete = asyncio.Future(loop = self.loop)
        self.__address = address
    
    
    def get_name(self):
        return self.__name
        
    
    def get_state(self):
        return self.__STATE
    
    
    def pre_start(self):
        logging.debug("Starting Actor {}".format(time.time()))

    
    def start(self):
        self.__STATE = ActorState.RUNNING
        self.loop.create_task(self._run())
    
    
    def _start(self):
        pass
    
    
    async def _run(self):
        while self.__STATE == ActorState.RUNNING:
            await self._task()
        self.__complete.set_result(True)
    
    
    async def stop(self):
        self.__STATE = ActorState.STOPPED
        await self._stop()
        await self.__complete
        self.post_stop()
        return True
        
    
    async def _stop(self):
        logging.log("Stopping Actor {}".format(time.time()))
    
     
    def post_stop(self):
        logging.debug("Actor Stopped {}".format(time.time()))
    
    
    def post_restart(self):
        logging.debug("Actor Restarted {}".format(time.time()))
    
    
    async def _task(self):
        raise NotImplementedError("The method receive Not Yet Implemented in Actor.")


    async def tell(self, target, message):
        try:
            await target._receive(message) 
        except AttributeError as ex:
            raise TypeError("Target Does not Have a _receive method. Is it an actor?") from ex
  
    
    async def ask(self, target, message):
        assert isinstance(message, QueryMessage)
        if not message.result:
            message.result = asyncio.Future(loop = self.loop)
        await self.tell(target, message)
        res = await message.result
        return res 


class BaseActor(AbstractActor):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.loop = kwargs.get('loop', self.loop)
        self._max_inbox_size = kwargs.get('max_inbox_size', 0)
        self._inbox = kwargs.get('queue', asyncio.Queue(maxsize=
                                self._max_inbox_size, loop = self.loop))
        self._handlers = {}

        # Create handler for the 'poison pill' message
        self.register_handler(PoisonPill, self._stop_message_handler)


    def register_handler(self, message_cls, func):
        self._handlers[message_cls] = func

    
    async def _task(self):
        message = await self._inbox.get()
        try:
            handler  = self._handlers[type(message)]
            is_query = isinstance(message, QueryMessage)
            try:
                response = await handler(message)
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
