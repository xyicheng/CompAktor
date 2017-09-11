'''
Created on Aug 30, 2017

@author: aevans
'''


import asyncio
import unittest
import pytest
import pytest_asyncio
from compaktor.flow.streaming import * 
from compaktor.actor.actor import ActorState
from compaktor.actor.message import PoisonPill

"""
Test Accounting Actors
"""

@pytest.mark.asyncio
async def test_accounting_tell():
    a = AccountingActor()
    a.start()
    assert a.get_state() == ActorState.RUNNING
    try:
        await a.tell(a, Demand({}))
    except Exception as e:
        pytest.fail("Failed to Send Blank Demand", pytrace = True)
    await a.stop()
    assert a.get_state() == ActorState.TERMINATED


def test_startup_teardown():
    a = AccountingActor()
    a.start()
    assert a.get_state() == ActorState.RUNNING
    a.loop.run_until_complete(a.stop())
    assert a.get_state() == ActorState.TERMINATED


def test_source_to_sink_connection():
    source = PrintRangeSource()
    source.start()
    skwargs = {'source' : source}
    account_actor = AccountingActor(*[], **skwargs)
    account_actor.start()
    kwargs = {'accounting_actor' : account_actor}
    sink = PrintRangeSink(*[], **kwargs)
    sink.start()
    source.subscribe(sink)
    while source.get_state() == ActorState.RUNNING:
        time.sleep(2)
    


class PrintRangeSource(Source):
    
    
    def __init__(self, *args, **kwargs):
        source_kwargs = {'pull_function' : self.on_pull}
        if kwargs is not None and len(kwargs.keys()) > 0:
            for k in kwargs:
                source_kwargs[k] = kwargs[k]
        super().__init__(*args, **source_kwargs)
        self.range = list(range(1,10))
        
    
    def on_pull(self):
        if len(self.range) > 0:
            return self.range.pop()
        else:
            return PoisonPill()


class PrintRangeSink(Sink):
    
    
    def __init__(self, *args, **kwargs):
        sink_kwargs = {'push_function' : self.on_push}
        if kwargs is not None and len(kwargs.keys()) > 0:
            for k in kwargs:
                sink_kwargs[k] = kwargs[k]
        super().__init__(*args, **sink_kwargs)

    
    def on_push(self, message):
        print(message)
 
 
class PrintRangeFilterStage(Stage):
     
     
    def __init__(self, *args, **kwargs):
        super().__init(*args, **kwargs)
    