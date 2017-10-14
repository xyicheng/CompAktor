'''
A thread safe queue with snychronized get operations.

Created on Oct 13, 2017

@author: aevans
'''


import logging
from multiprocessing import Lock
from queue import Queue


class BlockingQueue:
    """
    Blocking queue using locks. 
    """
    def __init__(self, max_size=1000):
        """
        Constructor

        :param max_size: Maximum size of the queue
        :type max_size: int()
        """
        self.__put_lock = Lock()
        self.__get_lock = Lock()
        self.__queue = Queue(maxsize=max_size)

    def sync_put(self, item, block=False):
        """
        Put into the queue

        :param item: The item to put into the queue
        :type item: object
        :param block: Whether to block on the queue
        :type block: bool()
        """
        self.__put_lock.acquire()
        try:
            if self.__queue.full() is False or block:
                print("Putting Item")
                self.__queue.put(item)
                print("Done Putting")
            else:
                logging.warn("Queue Full")
        finally:
            self.__put_lock.release()

    async def put(self, item):
        """
        Asynchronously put an item.

        :param item: The item to put
        :type item: object
        """
        self.sync_put(item)

    def sync_get(self, block=True):
        """
        Get from the queue synchronously

        :param block: Whether to block on the queue
        :type block: bool()
        :return: An item from the queue
        :type: object
        """
        print("In Sync Get")
        self.__get_lock.acquire()
        try:
            print("Getting")
            item = None
            if block or self.__queue.empty() is False:
                item = self.__queue.get()
            else:
                item = None
            print("Done Getting")
            return item
        finally:
            self.__get_lock.release()

    async def get(self):
        """
        Perform a get request

        :return: An item from the queue
        :rtype: object
        """
        return self.sync_get()
