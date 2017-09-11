'''
Created on Sep 9, 2017

@author: aevans
'''


import asyncio
import pytest
import time
from compaktor.actor.actor import ActorState
from compaktor.flow.streaming import Source, AccountingActor, Sink, Stage
from compaktor.flow.tests.stream_in_test import FlowRunner
from compaktor.actor.message import PoisonPill, Message


class Print(Message): pass


class PrintSource(Source):
    
    def __init__(self, *args, **kwargs):
        kwargs = {'pull_function' : self.pull}
        super().__init__(*args, **kwargs)
    
    def pull(self, message):
        pass 


class StageSource(Stage):
    
    def __init__(self, *args , **kwargs):
        kwargs = {}
        super().__init__(*args, **kwargs)

    
class PrintSink(Sink):
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.register_handler(Print, self.do_print)
    
    def do_print(self, message):
        print(message)


def test_closeable():
    """
    Test that the flow runner is closeable.
    """
    with FlowRunner() as runner:
        pass


def test_source_setup_and_terminate():
    """
    Test that the source gets setup appropriatley.
    """
    source = PrintSource()
    accounting_actor = AccountingActor()
    with FlowRunner() as runner:
        runner.create_new_flow(source, accounting_actor)
    asyncio.get_event_loop().run_until_complete(accounting_actor.stop())
    asyncio.get_event_loop().run_until_complete(source.stop())
    assert source.get_state() is ActorState.TERMINATED
    assert accounting_actor.get_state() is ActorState.TERMINATED


def test_sink_subscription():
    """
    Test that the sink works.
    """
    source = PrintSource()
    accounting_actor = AccountingActor()
    sink = PrintSink()
    with FlowRunner() as runner:
        runner.create_new_flow(source, accounting_actor)
        runner.to(sink)
    asyncio.get_event_loop().run_until_complete(accounting_actor.stop())
    asyncio.get_event_loop().run_until_complete(source.stop())
    asyncio.get_event_loop().run_until_complete(sink.stop)
    assert source.get_state() is ActorState.TERMINATED
    assert accounting_actor.get_state() is ActorState.TERMINATED
    assert sink.get_state() is ActorState.TERMINATED
    
    
def test_stage_subscription():
    """
    Test the stage subscription
    """
    source = PrintSource()
    accounting_actor = AccountingActor()
    sink = PrintSink()
    with FlowRunner() as runner:
        runner.create_new_flow(source, accounting_actor)
        runner.to(sink)
    asyncio.get_event_loop().run_until_complete(accounting_actor.stop())
    asyncio.get_event_loop().run_until_complete(source.stop())
    asyncio.get_event_loop().run_until_complete(sink.stop)
    assert source.get_state() is ActorState.TERMINATED
    assert accounting_actor.get_state() is ActorState.TERMINATED
    assert sink.get_state() is ActorState.TERMINATED


def test_print_flow():
    """
    Test the print flow
    """
    pass


def test_split_flow():
    """
    Test that the flow splits appropriately
    """
    pass


def test_mergeflow():
    """
    Test that the flow merges
    """
    pass


def test_map():
    """
    Test the map function
    """
    pass


def load_test():
    pass