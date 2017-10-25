'''
Actor Registry

Created on Oct 14, 2017

@author: aevans
'''

from test.actor import StringTestActor
from compaktor.state.actor_state import ActorState
from compaktor.registry.actor_registry import Registry
import unittest


class TestActorRegistry(unittest.TestCase):
    
    def test_add_actor_at_root(self):
        """
        Test and addition to the root of the registry.
        """
        registry = Registry("localhost")
        testa = StringTestActor("string_test")
        print(testa)
        testa.start()
        registry.add_actor("localhost", testa, is_local=True)
        node = registry.find_node(["localhost","string_test"])
        testa = node.actor
        test_arr = [x for x in testa.get_address()
                    if x not in ["localhost","string_test"]]
        assert(len(test_arr) is 0)
        assert(testa.get_address() != None)
        node = registry.find_node(testa.address)
        assert(node != None)
        registry.stop_all()
        assert(testa.get_state() == ActorState.TERMINATED)

    def test_multi_level_actor(self):
        registry = Registry("localhost")
        testa = StringTestActor("string_test")
        testa.start()
        testb = StringTestActor("second_level")
        testb.start()
        registry.add_actor("localhost", testa, is_local=True)
        registry.add_actor(["localhost","string_test"], testb, is_local=True)
        node = registry.find_node(["localhost","string_test"])
        testa = node.actor
        test_arr = [x for x in testa.get_address()
                    if x not in ["localhost","string_test"]]
        assert(len(test_arr) is 0)
        node = registry.find_node(["localhost","string_test","second_level"])
        testb = node.actor
        test_arr = [x for x in testb.get_address()
                    if x not in ["localhost","string_test","second_level"]]
        assert(len(test_arr) is 0)
        assert(testa.get_address() != None)
        node = registry.find_node(testa.address)
        assert(node != None)
        registry.stop_all()
        assert(testa.get_state() == ActorState.TERMINATED)
    
    def test_dual(self):
        registry = Registry("localhost")
        testa = StringTestActor("string_test")
        testa.start()
        testb = StringTestActor("string_test_b")
        testb.start()
        registry.add_actor("localhost", testa, is_local=True)
        registry.add_actor(["localhost"], testb, is_local=True)
        node = registry.find_node(["localhost","string_test"])
        testa = node.actor
        test_arr = [x for x in testa.get_address()
                    if x not in ["localhost","string_test"]]
        assert(len(test_arr) is 0)
        node = registry.find_node(["localhost","string_test_b"])
        testb = node.actor
        test_arr = [x for x in testb.get_address()
                    if x not in ["localhost","string_test_b"]]
        assert(len(test_arr) is 0)
        assert(testa.get_address() != None)
        node = registry.find_node(testa.address)
        assert(node != None)
        registry.stop_all()
        assert(testa.get_state() == ActorState.TERMINATED)

    def test_dual_multi_level(self):
        registry = Registry("localhost")
    
        testa = StringTestActor("string_test")
        testa.start()
        testb = StringTestActor("second_level")
        testb.start()
        registry.add_actor("localhost", testa, is_local=True)
        registry.add_actor(["localhost","string_test"], testb, is_local=True)
    
        testa = StringTestActor("string_test_b")
        testa.start()
        testb = StringTestActor("second_level_b")
        testb.start()
        registry.add_actor("localhost", testa, is_local=True)
        registry.add_actor(["localhost","string_test_b"], testb, is_local=True)
    
        node = registry.find_node(["localhost","string_test"])
        testa = node.actor
        test_arr = [x for x in testa.get_address()
                    if x not in ["localhost","string_test"]]
        assert(len(test_arr) is 0)
        node = registry.find_node(["localhost","string_test","second_level"])
        testb = node.actor
        test_arr = [x for x in testb.get_address()
                    if x not in ["localhost","string_test","second_level"]]
        assert(len(test_arr) is 0)
        
        
        node = registry.find_node(["localhost","string_test_b"])
        testa = node.actor
        test_arr = [x for x in testa.get_address()
                    if x not in ["localhost","string_test_b"]]
        assert(len(test_arr) is 0)
        node = registry.find_node(["localhost","string_test_b","second_level_b"])
        testb = node.actor
        test_arr = [x for x in testb.get_address()
                    if x not in ["localhost","string_test_b","second_level_b"]]
        assert(len(test_arr) is 0)
        
        assert(testa.get_address() != None)
        node = registry.find_node(testa.address)
        assert(node != None)
        registry.stop_all()
        assert(testa.get_state() == ActorState.TERMINATED)
        assert(testb.get_state() == ActorState.TERMINATED)
