'''
Created on Oct 19, 2017

@author: simplrdev
'''

from compaktor.streams.objects.node_pub_sub import NodePubSub
from compaktor.streams.objects.source import Source
from compaktor.streams.objects.sink import Sink
from abc import abstractmethod
from compaktor.multiprocessing import pool
from compaktor.multiprocessing.pool import multiprocessing_pool


class StringSource(Source):

    def __init__(self, src_num=0):
        super().__init__("StringSource")
        self.__iteration = 0
        self.src_num = src_num

    @abstractmethod
    def on_pull(self):
        print("Pulling")
        self.__iteration += 1
        return "TestPull {} {}".format(
            self.src_num, self.__iteration)

def do_split(message):
    return message[0].split(" ")
        
class SplitNode(NodePubSub):

    def __init__(self, providers=[]):
        super().__init__("SplitNode", providers)

    async def on_pull(self, message):
        print("Splitting {}".format(type(message)))
        fut = multiprocessing_pool.submit_process(func=do_split, args=(message, ))
        return fut.result()

class PrintSink(Sink):

    def __init__(self, providers=[]):
        super().__init__("PrintSink", providers)

    @abstractmethod
    def on_push(self, message):
        print(message.payload)
