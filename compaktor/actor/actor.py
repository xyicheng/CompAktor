'''
Basic actor classes.

Created on Aug 18, 2017

@author: aevans
'''


import time
import logging
from enum import Enum
import asyncio


class ActorState(Enum):
    STOPPED = 0
    RUNNING = 1
    LIMBO = 2
    TERMINATED = 3
    

class AbstractActor(object):
    
    
    __STATE = ActorState.DEAD
    
    
    def __init__(self, loop = None):
        self.loop = asyncio.get_event_loop() if loop is None else loop
        self.__STATE = ActorState.LIMBO
        self.__complete = asyncio.Future(loop = self.loop)
    
    
    def get_state(self):
        return self.__STATE
    
    
    def pre_start(self):
        logging.debug("Starting Actor {}".format(time.time()))

    
    def start(self):
        self.__STATE = ActorState.ALIVE
        self.loop.create_task(self._run())
    
    
    def _start(self):
        pass
    
    
    async def _run(self):
        while self.__STATE == ActorState.RUNNING:
            await self.receive()
        self.__complete.set_result(True)
    
    
    async def stop(self):
        self.__STATE = ActorState.STOPPED
        await self.__stop()
        await self.__complete
        self.post_stop()
        return True
        
    
    async def _stop(self):
        logging.log("Stopping Actor {}".format(time.time()))
    
     
    def post_stop(self):
        logging.debug("Actor Stopped {}".formatime(time.time()))
    
    
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
        assert isinstance(message, )