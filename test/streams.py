'''
Created on Oct 19, 2017

@author: simplrdev
'''

import unittest
import asyncio
from test.modules.streams.objects import StringSource, SplitNode, PrintSink
from compaktor.message.message_objects import PullQuery, Publish, Push, Pull
from compaktor.state.actor_state import ActorState


class TestStreams(unittest.TestCase):

    def test_stream_with_data(self):
        src = StringSource()
        src.start()
        node = SplitNode()
        node.add_provider(src)
        node.start()
        sn = PrintSink()
        sn.add_provider(node)
        sn.start()
        ps = Pull(None, sn)
        res = sn.loop.run_until_complete(
            sn.tell(node, ps))
        print(res)
        res = sn.loop.run_until_complete(
            sn.tell(node, ps))
        print(res)
        res = sn.loop.run_until_complete(
            sn.tell(node, ps))
        res = sn.loop.run_until_complete(
            sn.tell(node, ps))
        print(res)
        res = sn.loop.run_until_complete(
            sn.tell(node, ps))
        print(res)
        res = sn.loop.run_until_complete(
            sn.tell(node, ps))
        print("Done")
        print(res)
        #sink = PrintSink()
        #sink.add_source(node)
        #sink.start()
        asyncio.get_event_loop().run_forever()

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

    def stest_multi_loop(self):
        pass


if __name__ == "__main__":
    unittest.main()
