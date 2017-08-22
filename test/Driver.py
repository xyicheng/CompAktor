'''
Sadly, unit test is not working.  The first test is just ported over from
Cleveland.  Other tests include router tests, remote actor tests, and more.
Some tests may be chained but others cannot be.

Created on Aug 19, 2017

@author: aevans
'''


import asyncio
from multiprocessing import Process
import socket
import unittest
import aiounittest
from compaktor.actor.actor import BaseActor
from compaktor.actor.message import Message


class StringMessage(Message): pass


class IntMessage(Message): pass


class ObjectMessage(Message): pass


class ObjectTestActor(BaseActor):
    
    
    def __init__(self, *args, **kwargs):
        super.__init__(*args, **kwargs)
        self.register_handler(StringMessage, self.print_status)
        
    
    def print_status(self,message):
        print(message.payload) 


class StringTestActor(BaseActor):
    
    
    def __init__(self, *args, **kwargs):
        super.__init__(*args, **kwargs)
        self.register_handler(StringMessage, self.print_status)
        
    
    def print_status(self,message):
        print(message.payload) 
    

class AddTestActor(BaseActor):
    
    
    def __init__(self, *args, **kwargs):
        super.__init__(*args, **kwargs)
        self.register_handler(IntMessage, self.add_test)
        
    
    def add_test(self,message):
        return int.from_bytes(message.payload, byteorder='big', signed=False) + 1
        

class TestActor(unittest.TestCase):
    
    
    def test_setup(self):
        """
        The base actor takes your string and prints it.  Nothing is returned.
        A pass means that nothing faild.
        """
        a = BaseActor()
        b = StringTestActor()
        
        
    def test_serialization(self):
        """
        This uses the object message to ensure serialization.  
        """        
        pass
    

class TestActorSystem(unittest.TestCase): pass


class TestRoundRobinRouter(unittest.TestCase): pass


class TestBalancingRouter(unittest.TestCase): pass


class HealthCheckTester(unittest.TestCase): pass


if __name__ == "__main__":
    pass