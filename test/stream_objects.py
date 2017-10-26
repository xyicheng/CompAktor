'''
Created on Oct 26, 2017

@author: aevans
'''

import asyncio
import unittest
from test.modules.streams.objects import StringSource, LargeStringSource,\
                                        SplitNode, PrintSink
from compaktor.state.actor_state import ActorState
from compaktor.streams.objects.node_pub_sub import NodePubSub


class TestStreamObjects(unittest.TestCase):

    def test_source_setup(self):
        src = StringSource("src")
        src.start()
        assert(src.get_state() == ActorState.RUNNING)
        asyncio.get_event_loop().run_until_complete(src.stop())
        assert(src.get_state() == ActorState.TERMINATED)

    def test_node_setup(self):
        node_actor = SplitNode("split_node")
        node_actor.start()
        assert(node_actor.get_state() == ActorState.RUNNING)
        asyncio.get_event_loop().run_until_complete(node_actor.stop())
        assert(node_actor.get_state() == ActorState.TERMINATED)

    def stest_sink_setup(self):
        sink = PrintSink()
        sink.start()
        assert(sink.get_state() == ActorState.RUNNING)
        asyncio.get_event_loop().run_until_complete(sink.stop())
        assert(sink.get_state() == ActorState.TERMINATED)

    def test_source_node_connection(self):
        src = StringSource("src")
        src.start()
        node = SplitNode("split_node")
        node.add_source(src)
        node.start()
        asyncio.get_event_loop().run_until_complete(
            src.stop())
        asyncio.get_event_loop().run_until_complete(
            node.stop())
        assert(node.get_state() == ActorState.TERMINATED)
        assert(src.get_state() == ActorState.TERMINATED)

    def test_source_node_multi_loop_connection(self):
        src_loop = asyncio.new_event_loop()
        src = StringSource("src", loop=src_loop)
        src.start()
        n_loop = asyncio.new_event_loop()
        node = SplitNode("split_node", loop=n_loop)
        node.add_source(src)
        node.start()
        src.loop.run_until_complete(
            src.stop())
        node.loop.run_until_complete(
            node.stop())
        assert(node.get_state() == ActorState.TERMINATED)
        assert(src.get_state() == ActorState.TERMINATED)

    def test_node_sink_connection(self):
        node = SplitNode("split_node")
        node.start()
        ps = PrintSink("print_sink")
        ps.add_source(node)
        ps.start()
        print("Stopping Node")
        asyncio.get_event_loop().run_until_complete(
            node.stop())
        print("Stopping Sink")
        asyncio.get_event_loop().run_until_complete(ps.stop())
        print("Checking States")
        assert(node.get_state() == ActorState.TERMINATED)
        assert(ps.get_state() == ActorState.TERMINATED)

    def test_node_sink_multi_loop_connection(self):
        node_loop = asyncio.new_event_loop()
        node = SplitNode("split_node", loop=node_loop)
        node.start()
        ps_loop = asyncio.new_event_loop()
        ps = PrintSink("print_sink", loop=ps_loop)
        ps.add_source(node)
        ps.start()
        print("Stopping Node")
        node.loop.run_until_complete(node.stop())
        print("Stopping Sink")
        ps.loop.run_until_complete(ps.stop())
        print("Checking States")
        assert(node.get_state() == ActorState.TERMINATED)
        assert(ps.get_state() == ActorState.TERMINATED)


if __name__ == "__main__":
    unittest.main()
