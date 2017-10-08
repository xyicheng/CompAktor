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


class FileSource(Source):
    """
    A source that obtains and uses paths from a FileObject
    """

    def __init__(self):
        pass