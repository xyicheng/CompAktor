'''
Created on Oct 19, 2017

@author: simplrdev
'''

import unittest
import asyncio
from threading import Thread
from test.modules.streams.objects import StringSource, SplitNode, PrintSink


class TestStreams(unittest.TestCase):

    def test_stream_with_data(self):
        src_loop = asyncio.get_event_loop()
        src = StringSource(loop=src_loop)
        src.start()
        #src_loopb = asyncio.get_event_loop()
        srcb = StringSource(src_num=1, loop=src_loop)
        srcb.start()

        #do_loop = asyncio.new_event_loop()
        #node = SplitNode(loop=do_loop)
        #node.add_actor(src)
        #node.add_actor(srcb)
        #node.start()

        ps_loop = asyncio.get_event_loop()
        ps = PrintSink(loop=ps_loop)
        ps.add_provider(src)
        ps.add_provider(srcb)
        asyncio.get_event_loop().run_until_complete(ps.start())
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
