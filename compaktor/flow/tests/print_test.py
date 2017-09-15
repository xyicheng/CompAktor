'''
Created on Sep 9, 2017

@author: aevans
'''


import asyncio
import pytest
import time
from compaktor.actor.actor import ActorState
from compaktor.flow.streaming import Source, AccountingActor, Sink, Stage,\
    TickActor, Tick, Pull, FlowResult
from compaktor.flow.tests.stream_in_test import FlowRunner
from compaktor.actor.message import PoisonPill, Message
import time
from compaktor.connectors.pub_sub import Publish, Subscribe, PubSub


class Print(Message):
    pass


class PrintSource(Source):
    
    def __init__(self, *args, **kwargs):
        kwargs = {'pull_function' : self.pull}
        super().__init__(*args, **kwargs)
        self.register_handler(Subscribe, self.subscribe)

    def pull(self, message):
        return "Hello World!"


class StageSource(Stage):

    def __init__(self, *args , **kwargs):
        kwargs = {}
        super().__init__(*args, **kwargs)


class PrintSink(Sink):
    
    def __init__(self, *args, **kwargs):
        new_args = kwargs
        if new_args is None:
            new_args = {}
        new_args['push_function'] = self.do_print
        
        acct_actor = AccountingActor()
        acct_actor.start()
        new_args['accounting_actor'] = acct_actor
    
        super().__init__(*args, **new_args)
        self.register_handler(FlowResult, self.do_print)
        self.register_handler(Print, self.do_print)
        self.register_handler(Publish, self.do_print)

    async def do_print(self, message):
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
    asyncio.get_event_loop().run_until_complete(sink.stop())
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


async def test_tick(actor): 
    await actor.tell(actor, Tick())


async def test_subscribe(pub, sub):
    await sub.tell(pub, Subscribe(sub))


if __name__ == "__main__":
    source = PrintSource()
    source.start()
    accounting_actor = AccountingActor()
    kwargs = {'source' : source, 'accounting_actor' : accounting_actor}
    tick_actor = TickActor(*[], **kwargs)
    tick_actor.start()
    sink = PrintSink()
    asyncio.get_event_loop().run_until_complete(test_subscribe(source, sink))
    while True:
        asyncio.get_event_loop().run_until_complete(test_tick(tick_actor))
