'''
Created on Oct 14, 2017

@author: aevans
'''

import pytest
from test.actor import StringTestActor
from compaktor.state.actor_state import ActorState
from compaktor.registry.actor_registry import Registry


def test_add_actor_at_root():
    registry = Registry("localhost")
    testa = StringTestActor("string_test")
    testa.start()
    addr = testa.get_address()
    registry.add_actor("localhost", testa, is_local=True)
    test_arr = [x for x in addr if x not in ["localhost","testa"]]
    assert(len(test_arr) is 0)
    assert(testa.get_address() != None)
    node = registry.find_node(testa.address)
    assert(node != None)
    registry.stop_all()
    assert(testa.get_state() == ActorState.STOPPED)


def test_multi_level_actor():
    pass


def test_load():
    pass


if __name__ == "__main__":
    test_add_actor_at_root()
