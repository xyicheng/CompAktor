'''
Created on Oct 14, 2017

@author: aevans
'''

import pytest
from test.actor import StringTestActor
from compaktor.registry.actor_registry import Registry


def test_add_actor():
    registry = Registry()
    testa = StringTestActor("string_test")
    testa.start()
    registry.add_actor("root/testa", actor, is_local)
