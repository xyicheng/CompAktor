'''
A stage for mapping values.
Created on Oct 10, 2017

@author: aevans
'''

import asyncio
from compaktor.streams.objects.NodePubSub import NodePubSub
from compaktor.etl.objects.row import RowVector


class MapStage(NodePubSub):

    def __init__(self, name, providers, mappings, reg_mappings=None,
                 loop=asyncio.get_event_loop(), address=None, mailbox_size=1000,
                 inbox=None, empty_demand_logic = "broadcast"):
        """
        Constructor

        :param name: The name of the stage
        :type name: str()
        :param providers: A list of providers for the system
        :type providers:  list()
        :param mappings: The direct translations {'col':{'a':b}}
        :type mappings: dict()
        :param reg_mappings: Dictionary of regular expression mappings
        :type reg_mappings: The regular expression mappings
        :param loop: The AbstractEventLoop
        :type loop: AbstractEventLoop()
        :param address: The actor address
        :type address: str()
        :param mailbox_size:  The max size of the mailbox
        :type mailbox_size: int()
        :param inbox: The inbox type
        :type inbox: Queue()
        :param empty_demand_logic: braodcast or round_robin
        :type empty_demand_logic: str()
        """
        super().__init__(name, providers, loop, address, mailbox_size, inbox,
                         empty_demand_logic)
        self.__mappings = mappings
        self.__reg_mappings = reg_mappings
        self.__mappings = mappings

    def on_pull(self, message):
        """
        Called from the pull function.  User implementation.

        :param message: The message to use
        :type message: RowVector()
        """
        try:
            payload = message.payload
            if isinstance(payload, RowVector):
                if self.__reg_mappings:
                    
        except Exception as e:
            self.handle_fail()
