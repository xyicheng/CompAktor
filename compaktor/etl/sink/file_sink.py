'''
A sink that writes to a file
Created on Oct 7, 2017

@author: aevans
'''

import asyncio
import logging
import os
import re
from compaktor.streams.objects import Sink
from compaktor.actor.pub_sub import PubSub
from compaktor.message.message_objects import Publish
from compaktor.io.file_stream import OutputStream

class FileSink(Sink):
    """
    File sink for writing out to a file.
    """

    def __init__(self, name, fdir, fname, loop=asyncio.get_event_loop(),
                 address=None, mailbox_size=1000, inbox=None,
                 max_part_size=5000, ftype=".txt", is_bytes=True):
        """
        File sink

        :param name: The name of the sink
        :type name: str()
        :param fdir: File directory for parts
        :type fdir: str()
        :param fname: The base file name
        :type fname: str()
        :param loop: The event loop for the sink
        :type loop: AbstractEventLoop()
        :param address: Actor address
        :type address: str()
        :param mailbox_size: Maximum mailbox size
        :type mailbox_size: int()
        :param inbox: The inbox queue
        :type inbox: Queue()
        :param max_part_size: Maximum file part size
        :type max_part_size:  int()
        :param ftype: File type
        :type ftype: str()
        :param is_bytes: Whether to write byte to the file
        :type is_bytes: bool()
        """
        super().__init__(name, loop, address, mailbox_size, inbox)
        self.file = None
        self.__ftype = ftype
        self.__fopen = None
        self.__fname = fname
        self.__fdir = fdir
        self.__max_part_size = max_part_size
        self.__current_part = 0
        self.__current_part_size = 0
        self.__is_bytes = is_bytes
        if self.__fdir is None or os.path.exists(self.__fdir) is False:
            err_msg = "File Directory {} Does Not Exist".format(self._fdir)
            raise ValueError(err_msg)

        if self.__fname is None:
            raise ValueError("File Name is None")
        self.__fname = re.sub("\\..*", "", self.__fname)
        self.__dir_parts = self.__fdir
        if isinstance(self.__fdir, str):
            self.__dir_parts = self.__fdir.split(os.path.sep)

    def __open_stream(self):
        """
        Open a new stream.
        """
        try:
            if self.__fopen and self.__fopen.is_open:
                self.__fopen.close()

            split_fparts = [x for x in self.__dir_parts]
            ufname = "{}_part{}".format(
                self.__fname, self.__current_part)
            if self.__ftype is not None:
                ufname = "{}{}".format(ufname, self.__ftype)
            split_fparts.append(ufname) 
            ufpath = split_fparts.join(os.path.sep) 
            stream = None
            if self.__is_bytes:
                stream = open(ufpath, 'wb')
            else:
                stream(ufpath, 'w')
            self.__fopen = OutputStream(stream)
            self.__current_part_size = 0
            self.__current_part += 1
        except Exception as e:
            self.handle_fail()

    def __close_current_stream(self):
        try:
            if self.__fopen:
                self.__fopen.close()
        except Exception as e:
            self.handle_fail()

    def on_push(self, message):
        """
        The file write function.

        :param message: Message to process
        :type message: str()
        """
        try:
            proc = True
            out_line = message.payload
            if isinstance(out_line, str) and self.__is_bytes:
                out_line = bytearray(out_line)
            elif isinstance(out_line, bytearray) and self.__is_bytes is False:
                out_line = str(out_line)
            elif isinstance(out_line, bytearray) is False and\
            isinstance(out_line, str) is False:
                proc = False
                err_msg = "Payload to File Sink Must be String or Byte Array"
                logging.error(err_msg)

            if proc:
                if isinstance(message, Publish) is False:
                    err_msg = "Message is not of Type FlowResult in Sink"
                    logging.error(err_msg)
                elif out_line is not None:
                    if self.__fopen is None or\
                     self.__current_part_size >= self.__max_part_size:
                        self.__open_stream()
    
                    if self.__fopen:
                        self.__fopen.write(out_line)
        except Exception as e:
            self.handle_fail()
