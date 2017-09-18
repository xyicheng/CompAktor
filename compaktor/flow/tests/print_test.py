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
import functools


class Print(Message):
    pass


class PrintSource(Source):
    
    def __init__(self, *args, **kwargs):
        kwargs = {'pull_function' : self.pull}
        super().__init__(*args, **kwargs)
        self.register_handler(Subscribe, self.subscribe)

    def pull(self, message):
        return "Hello World!"


class StringManipulationStage(Stage):

    def __init__(self, *args , **kwargs):
        ukwargs = {'func' : self.manipulation_func}
        for arg in kwargs:
            ukwargs[arg] = kwargs[arg]
        super().__init__(*args, **ukwargs)
        
    def manipulation_func(self, message):
        in_str = message.payload
        return "Manipulating {}".format(in_str) 


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
        print(message.payload)


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
    async def shutdown_system(source):
        await source.tell(source, PoisonPill()) 
    source = PrintSource()
    source.start()
    accounting_actor = AccountingActor()
    kwargs = {'source' : source, 'accounting_actor' : accounting_actor,
              'tick_time' : 0}
    tick_actor = TickActor(*[], **kwargs)
    tick_actor.start()
    sink = PrintSink()
    source.get_publisher().subscribe(sink)
    asyncio.get_event_loop().run_until_complete(shutdown_system(source))


def test_stage_connection():
    async def shutdown_system(source):
        await source.tell(source, PoisonPill()) 
    accounting_actor = AccountingActor()
    skwargs = {'accounting_actor' : accounting_actor}
    source = PrintSource(*[], **skwargs)
    source.start()
    accounting_actor = AccountingActor()
    kwargs = {'source' : source, 'accounting_actor' : accounting_actor,
              'tick_time' : 0}
    stage = StringManipulationStage(*[], **kwargs)
    source.get_publisher().subscribe(stage)
    tick_actor = TickActor(*[], **kwargs)
    tick_actor.start()
    sink = PrintSink()
    stage.get_publisher().subscribe(sink)
    asyncio.get_event_loop().run_until_complete(shutdown_system(source))

def test_multi_stage_connection():
    async def shutdown_system(source):
        await source.tell(source, PoisonPill()) 
    accounting_actor = AccountingActor()
    skwargs = {'accounting_actor' : accounting_actor}
    source = PrintSource(*[], **skwargs)
    source.start()
    accounting_actor = AccountingActor()
    kwargs = {'source' : source, 'accounting_actor' : accounting_actor,
              'tick_time' : 0}
    stage = StringManipulationStage(*[], **kwargs)
    source.get_publisher().subscribe(stage)
    stageb = StringManipulationStage(*[], **kwargs)
    stage.get_publisher().subscribe(stageb)
    stagec = StringManipulationStage(*[], **kwargs)
    stageb.get_publisher().subscribe(stagec)
    staged = StringManipulationStage(*[], **kwargs)
    stagec.get_publisher().subscribe(staged)
    stagee = StringManipulationStage(*[], **kwargs)
    staged.get_publisher().subscribe(stagee)
    source.get_publisher().subscribe(stage)
    tick_actor = TickActor(*[], **kwargs)
    tick_actor.start()
    sink = PrintSink()
    stagee.get_publisher().subscribe(sink)
    asyncio.get_event_loop().run_until_complete(shutdown_system(source))

async def test_tick(actor):
    await actor.tell(actor, Tick())


async def test_subscribe(pub, sub):
    await sub.tell(pub, Subscribe(sub))


if __name__ == "__main__":
    accounting_actor = AccountingActor()
    
    asyncio.get_event_loop().run_forever()
