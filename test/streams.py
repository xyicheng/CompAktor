'''
Stream tests.

Created on Sep 19, 2017

@author: aevans
'''


import pytest
from compaktor.actor.actor import BaseActor
from compaktor.actor.message import Message
from compaktor.flow.streaming import Source, Sink


class TestMessage(Message):
    pass


class TestSource(Source):
    
    def __init__(self, *args, **kwargs):
        skwargs = kwargs
        if skwargs:
            skwargs = {'pull_function' : self.do_pull}
        else:
            skwargs["pull_function"] = self.do_pull

        super().__init__(args, kwargs)
        self.register_handler(TestMessage, self.do_pull)

    def do_pull(self):
        pass


class TestSink(Sink):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        

@pytest.fixture(scope="module")
def flow_tuple():
    source  = T
    Flow()
    return ()

def test_stream_intantiation():
    """
    Test stream instantiation. 
    """
    pass


def test_text_tiling_flow():
    """
    Directory specific.  Inputs data as outlined here.
    Download by searching American Poli at
    https://app.scrapinghub.com/datasets
    """
    pass

def test_at_load():
    """
    Test data at load
    """
    pass


def test_multi_flow():
    """
    Test mltiple flows (sub-flows)
    """
    pass

def test_multi_flow_at_load():
    """
    Test mutltiple flows against a heavy load
    """
    pass
