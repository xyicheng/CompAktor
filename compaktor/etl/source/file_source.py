'''
Stream source
Created on Sep 21, 2017

@author: aevans
'''

import asyncio
import os
from queue import Queue
from compaktor.streams.objects import Source
from compaktor.io.file import File
from compaktor.actor.pub_sub import PubSub

import asyncio
import logging
import os
import re
from compaktor.streams.objects import Sink
from compaktor.actor.pub_sub import PubSub
from compaktor.message.message_objects import Publish
from compaktor.io.file_stream import OutputStream


class FileSource(Source):
    """
    A source that obtains and uses paths from a FileObject
    """

    def __init__(self, name, base_dir, loop=asyncio.get_event_loop(),
                 address=None, mailbox_size=1000, inbox=None, skip_regex=None,
                 is_bytes=True, routing_logic="round_robin"):
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
        :param skip_regex: File regex for files to skip
        :type skip_regex: str()
        :param is_bytes: Whether to read as a bytearray
        :type is_bytes: bool()
        :param routing_logic: The logic to use when routing
        :type routing_logic: str()
        """
        super().__init__(name, loop, address,
                 mailbox_size, inbox, routing_logic)
        self.file = None
        self.__fopen = None
        self.__base_dir = base_dir
        self.__is_bytes = is_bytes
        self.__skip_regex = skip_regex
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

    def on_pull(self, message):
        pass
