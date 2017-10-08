'''
Created on Oct 1, 2017

@author: aevans
'''


import pytest
from compaktor.streams.objects.sink import Sink
from compaktor.streams.graph import Source, BaseStage
from compaktor.streams.graph import GraphManager


@pytest.fixture(scope="module")
def graph():
    return GraphManager()


def test_source_creation(graph):
    """
    Test source creation

    @param graph: 
    """
    source = Source()
    graph.connect_source("test_source", source, edges=[])
    graph.close_graph(10)


def test_stage_creation(graph):
    nodea = BaseStage()
    source = Source()
    graph.connect_source("test_source", source, edges=[])
    edge = BaseStage()
    graph.add_node("node_a", nodea, "test_source")
    graph.close_graph(10)


def test_source_attachment(graph):
    nodea = BaseStage()
    source = Source()
    graph.connect_source("test_source", source, edges=[])
    edge = BaseStage()
    graph.add_node("node_a", nodea, "test_source")
    graph.close_graph(10)


def test_sink_connection(graph, start_name, actora, sink_name):
    # nodea = BaseStage()
    # source = Source()
    # graph.connect_source("test_source", source, edges=[])
    # edge = BaseStage()
    # graph.add_node("node_a", nodea, "test_source")
    # sink = Sink()
    # graph.add_node(start_name, actora, end_name)
    # graph.close_graph(10)
    pass
