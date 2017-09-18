'''
A multi-threaded loop pool.


Created on Sep 17, 2017

@author: aevans
'''


from concurrent.futures import ThreadPoolExecutor
from threading import Thread
import asyncio
from asyncio.events import AbstractEventLoop, Handle
import functools


def run_loop(loop):
    asyncio.set_event_loop(loop)
    loop.run_forever()


class LoopThreadPool:
    """
    This thread pool is made to manage the event loops.  Other thread pools
    should be used for executing tasks. 
    """
    
    def __init__(self):
        """
        Constructor
        """
        self.__loops = []
        self.__threads = []
    
    def create_loop(self):
        """
        Creates an event loop running in a separate thread.

        :return: The generated loop
        :rtype: AsbractEventLoop()
        """
        loop = asyncio.new_event_loop()
        loop_t = Thread(target=run_loop, args=[loop])
        loop_t.start()
        self.__threads.append(loop_t)
        self.__loops.append(loop)
        return loop

    def get_num_threads(self):
        """
        Get the number of stored threads
        """
        return len(self.__threads)

    def remove(self, loop):
        """
        Remove a loop from the loops list. Attempts to shutdown a loop if
        isRunning returns True.
        
        :param loop: The loop to shutdown and remove
        :type loop: AbstractEventLoop()
        """
        if loop.is_running():
            loop.call_soon_threadsafe(functools.partial(loop.stop))
        self.__loops.remove(loop)

    def shutdown(self, timeout = 10):
        """
        Shutdown the threadpool, first closing loops.
        
        """
        for loop in self.__loops:
            if loop.is_running():
                loop.call_soon_threadsafe(functools.partial(loop.stop))
        self.__loops = []
        
        for thread in self.__threads:
            try:
                thread.join()
            except Exception as e:
                print(e.getMessage())
