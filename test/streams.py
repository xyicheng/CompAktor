'''
Created on Oct 19, 2017

@author: simplrdev
'''

import unittest
import asyncio
from test.modules.streams.objects import StringSource, SplitNode, PrintSink
from compaktor.message.message_objects import PullQuery, Publish
from compaktor.state.actor_state import ActorState
from compaktor.streams.objects.node_pub_sub import NodePubSub

class TestStreams(unittest.TestCase):

    def stest_source_start(self):
        src = StringSource()
        src.start()
        assert(src.get_state() == ActorState.RUNNING)
        for i in range(0, 10):
            asyncio.get_event_loop().run_until_complete(
                src.ask(src, PullQuery()))
        asyncio.get_event_loop().run_until_complete(
            src.stop())
        assert(src.get_state() == ActorState.TERMINATED)

    def stest_sink_start(self):
        sink = PrintSink()
        sink.start()
        assert (sink.get_state() == ActorState.RUNNING)
        for i in range(0, 10):
            pub = Publish("Test {}".format(i),None)
            asyncio.get_event_loop().run_until_complete(
                sink.tell(sink, pub))
        asyncio.get_event_loop().run_until_complete(
            sink.stop())

    def stest_node_start(self):
        sn = SplitNode()
        sn.start()
        assert(sn.get_state() == ActorState.RUNNING)
        asyncio.get_event_loop().run_until_complete(
            sn.stop())
        assert(sn.get_state() == ActorState.TERMINATED)

    def stest_stream_setup(self):
        src = StringSource()
        src.start()
        sn = SplitNode()
        sn.start()
        ps = PrintSink()
        ps.start()
        asyncio.get_event_loop().run_until_complete(src.subscribe(sn))
        asyncio.get_event_loop().run_until_complete(sn.subscribe(ps))
        asyncio.get_event_loop().run_until_complete(ps.stop())
        asyncio.get_event_loop().run_until_complete(sn.stop())
        asyncio.get_event_loop().run_until_complete(src.stop())
        assert(src.get_state() == ActorState.TERMINATED)
        assert(ps.get_state() == ActorState.TERMINATED)
        assert(sn.get_state() == ActorState.TERMINATED)

    def test_stream_with_data(self):
        src = StringSource()
        src.start()
        sn = SplitNode(providers=[src])
        sn.start()
        ps = PrintSink(providers=[sn])
        ps.start()
        asyncio.get_event_loop().run_forever()

    def stest_multi_input_stream(self):
        pass

    def stest_split_stream(self):
        pass

    def stest_multi_sink_stream(self):
        pass

    def stest_multi_source_stream(self):
        pass

    def stest_queu_sink(self):
        pass

    def stest_multi_queue_sink(self):
        pass


if __name__ == "__main__":
    unittest.main()
