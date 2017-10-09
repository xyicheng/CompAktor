'''
Stream source
Created on Sep 21, 2017

@author: aevans
'''

import asyncio
import os
from queue import Queue as PyQueue
from compaktor.streams.objects import Source
from compaktor.io.file import File
from compaktor.actor.pub_sub import PubSub

import asyncio
import logging
import os
import re
from compaktor.streams.objects import Sink
from compaktor.actor.pub_sub import PubSub
from compaktor.message.message_objects import Publish, Pull
from compaktor.io.file_stream import OutputStream, InputStream
from compaktor.actor.abstract_actor import AbstractActor


class FileSource(Source):
    """
    A source that obtains and uses paths from a FileObject
    """

    def __init__(self, name, base_dir, loop=asyncio.get_event_loop(),
                 address=None, mailbox_size=1000, inbox=None, skip_regex=None,
                 is_bytes=True, routing_logic="round_robin", on_finish=None):
        """
        Contructor

        :param name: File source name
        :type name: str()
        :param base_dir: The base directory
        :param loop: The event loop for the source
        :type loop: AbstractEventLoop()
        :param address: The actor address
        :type address: str()
        :param mailbox_size: Maximum inbox size
        :type mailbox_size: int()
        :param inbox: The inbox
        :type inbox: Queue()
        :param skipregex: File regex for files to skip
        :type skip_regex: str()
        :param is_bytes: Whether to read as a bytearray
        :type is_bytes: bool()
        :param routing_logic: The logic to use when routing
        :type routing_logic: str()
        :param on_finish: The callback to use on finish
        :type on_finish: method or function
        """
        super().__init__(name, loop, address,
                 mailbox_size, inbox, routing_logic)
        self.file = None
        self.__fopen = None
        self.__base_dir = base_dir
        self.__is_bytes = is_bytes
        self.__skip_regex = skip_regex
        self.__f_queue = PyQueue()
        self.__on_finish = on_finish
        if self.__base_dir is None or os.path.exists(self.__base_dir) is False:
            err_msg = "Base Directory {} Does Not Exist"
            err_msg = err_msg.format(self.__base_dir)
            err_msg = err_msg.format(err_msg)
            raise ValueError(err_msg)
        if self.__fname is None:
            raise ValueError("File Name is None")
        self.__dir_parts = self.__fdir
        if isinstance(self.__fdir, str):
            self.__dir_parts = self.__fdir.split(os.path.sep)
        self.__populate_queue()
            
    def __populate_queue(self):
        base_parts = self.__base_dir.split(os.path.sep)
        new_dirs = PyQueue()
        new_dirs.put(self.__base_dir)
        while new_dirs.empty() is False:
            pot_files = os.listdir(path=new_dirs.get_nowait())
            for file in pot_files:
                upath = [x for x in base_parts]
                upath.append(file)
                if os.path.isfile(upath):
                    self.__f_queue.put(upath)
                else:
                    no_skip = (self.__skip_regex and\
                               re.match(self.__skip_regex, upath))
                    if no_skip or self.__skip_regex is None:
                        new_dirs.put_nowait(upath)

    def on_pull(self, message):
        """
        File source pull function

        :param message: The message for the pull
        :type message: Pull()
        """
        try:
            if isinstance(message, Pull):
                payload = message.payload
                if isinstance(payload, AbstractActor):
                    if self.__fopen is None or self.__fopen.has_next() is False:
                        if self.__fopen.is_open:
                            self.__fopen.close()
                        if self.__f_queue.empty() is False:
                            fpath = self.__f_queue.get_nowait()
                            stream = open(fpath)
                            self.__fopen = InputStream(stream)
                        elif self.__on_finish is not None:
                            self.__on_finish()

                    if self.__fopen and self.__fopen.is_open:
                        if self.__fopen.has_next():
                            return self.__fopen.next()
                else:
                    err_msg = "Pull in Source Only Accepts Abstract Actor"
                    logging.error(err_msg)
            else:
                logging.error("Message must be instance of Pull in Source")
        except Exception as e:
            self.handle_fail()
