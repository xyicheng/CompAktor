'''
Created on Oct 19, 2017

@author: simplrdev
'''

import unittest
import asyncio
from test.modules.streams.objects import StringSource, SplitNode, PrintSink
from compaktor.message.message_objects import Pull, PullQuery, Publish
import pdb
from compaktor.state.actor_state import ActorState

class TestStreams(unittest.TestCase):

    def test_source_start(self):
        src = StringSource()
        src.start()
        assert(src.get_state() == ActorState.RUNNING)
        for i in range(0, 10):
            asyncio.get_event_loop().run_until_complete(src.ask(src, PullQuery()))
        asyncio.get_event_loop().run_until_complete(src.stop())
        assert(src.get_state() == ActorState.TERMINATED)

    def test_sink_start(self):
        sink = PrintSink()
        sink.start()
        assert (sink.get_state() == ActorState.RUNNING)
        for i in range(0, 10):
            pub = Publish("Test {}".format(i),None)
            asyncio.get_event_loop().run_until_complete(sink.tell(sink, pub))
        asyncio.get_event_loop().run_until_complete(sink.stop())

    def test_node_start(self):
        sn = SplitNode()
        sn.start()
        
        

    def test_stream_setup(self):
        pass

    def test_string_split(self):
        pass
    

if __name__ == "__main__":
    unittest.main()
