'''
Created on Oct 1, 2017

@author: aevans
'''


import pytest
from compaktor.streams.graph import GraphManager


@pytest.fixture(scope="module")
def graph():
    return GraphManager()

def test_source_creation():
    pass
