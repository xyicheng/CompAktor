'''
A thread safe queue with snychronized get operations.

Created on Oct 13, 2017

@author: aevans
'''

import asyncio
import logging
from janus import Queue


class BlockingQueue:
    """
    Blocking queue using locks. 
    """
    def __init__(self, max_size=1000, loop=asyncio.get_event_loop()):
        """
        Constructor

        :param max_size: Maximum size of the queue
        :type max_size: int()
        """
        self.__jq = Queue(maxsize=max_size)
        self.__queue = self.__jq.async_q

    def close(self):
        self.__jq.close()

    async def put(self, item, block=False):
        """
        Put into the queue

        :param item: The item to put into the queue
        :type item: object
        :param block: Whether to block on the queue
        :type block: bool()
        """
        if self.__queue.full() is False or block:
            print("Putting Item")
            await self.__queue.put(item)
            print("Done Putting")
        else:
            logging.warn("Queue Full")

    async def get(self, block=True):
        """
        Get from the queue synchronously

        :param block: Whether to block on the queue
        :type block: bool()
        :return: An item from the queue
        :type: object
        """
        print("Getting")
        item = None
        if block or self.__queue.empty() is False:
            item = await self.__queue.get()
        else:
            item = None
        print("Done Getting")
        return item
