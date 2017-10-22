'''
Created on Oct 19, 2017

@author: simplrdev
'''

from compaktor.streams.objects.node_pub_sub import NodePubSub
from compaktor.streams.objects.source import Source
from compaktor.streams.objects.sink import Sink
from abc import abstractmethod


class StringSource(Source):

    def __init__(self):
        super().__init__("StringSource")
        self.__iteration = 0

    @abstractmethod
    def on_pull(self):
        print("Pulling")
        return "TestPull {}".format(self.__iteration)

class SplitNode(NodePubSub):

    def __init__(self, providers=[]):
        super().__init__("SplitNode", providers)

    def on_pull(self, message):
        payload = str(message.payload)
        return payload.split(" ")

class PrintSink(Sink):

    def __init__(self, providers=[]):
        super().__init__("PrintSink", providers)

    @abstractmethod
    def on_push(self, message):
        print(message.payload)
