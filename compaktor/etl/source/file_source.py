'''
Stream source
Created on Sep 21, 2017

@author: aevans
'''

import asyncio
import os
from queue import Queue
from compaktor.streams.objects.source import Source
from compaktor.io.file import File
from compaktor.actor.pub_sub import PubSub
from compaktor.io.file_stream import Stream


class FileSource(Source):
    """
    A source that obtains and uses paths from a FileObject
    """

    def __init__(self, source_path, loop=asyncio.get_event_loop(), address=None,
                 mailbox_size=1000, inbox=None, pub=PubSub(), name = "Source"):
        """
        File source

        :param source_path: Initial directory path
        :type source_path: str()
        :param loop: The asyncio loop to use
        :type loop: AbstractEventLoop()
        :param address: Actor address
        :type address: str()
        :param mailbox_size: The size of the mailbox
        :type mailbox_size: int()
        :param inbox: The inbox queue
        :type inbox: Queue()
        :param pub: The publisher subscriber
        :type pub: PubSub()
        :param name: The source name
        :type name: str()
        """
        self.__file_handler = None
        self.queue = Queue()
        super().__init__(self.pull_func, name, loop, address, mailbox_size,
                         inbox, pub)

    def setup(self, source_path, fq):
        """
        Put files in the file queue.

        :param source_path: The path to the root of the source directory
        :type source_path: str()
        :param fq: The file queue
        :type fq: Queue
        :return: The queue with file paths added
        :rtype: Queue()
        """
        new_dirs = Queue()
        
        while new_dirs.empty() is False:
            dir = new_dirs.get_nowait()
            l_dirs = os.listdir(path=dir)
            for dir in l_dirs:
                f_parts = [x for x in l_dirs]
                f_parts.append(dir)
                new_dirs.put(File(file_path_parts=f_parts))

    def pull_func(self, message):
        """
        The pull function for the source.
        """
        payload = message.payload
        if self.file_handler is None or not self.__file_handler.has_next():
            if self.queue.empty() is False:
                self.__file_handler = Stream(self.queue.get_nowait())
            else:
                self.__stream.close()
                self.on_complete()
        if self.__file_handler or self.__file_handler.has_next():
            return self.__file_handler.next()
