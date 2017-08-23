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
from compaktor.actor.message import Message, QueryMessage


async def stop_actor(a):
    await a.stop()


class StringMessage(Message): pass


class IntMessage(Message): pass


class AddIntMessage(QueryMessage): pass


class ObjectMessage(Message): pass


class ObjectTestActor(BaseActor):
    
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.register_handler(ObjectMessage, self.print_status)
        
    
    def print_status(self,message):
        print("Received Payload")
        print(message.__repr__()) 


class StringTestActor(BaseActor):
    
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.register_handler(StringMessage, self.print_status)
        
    
    def print_status(self,message):
        print(message.payload) 
    

class AddTestActor(BaseActor):
    
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.register_handler(AddIntMessage, self.add_test)
        
    
    async def add_test(self,message):
        return message.payload + 1
        

class ActorTest(unittest.TestCase):    
    
    
    '''
    def test_serialization(self):
        """
        This uses the object message to ensure serialization.  
        """        
        async def test():
            a = ObjectTestActor()
            b = BaseActor()
            a.start()
            b.start()
            print("\nTelling")
            message = ObjectMessage(10)
            await asyncio.sleep(0.25)
            await b.tell(a, message)
            await a.stop()
            await b.stop()
        asyncio.get_event_loop().run_until_complete(test())
    
    
    def test_setup(self):
        """
        Test actor setup
        """
        async def test():
            a = BaseActor()
            b = StringTestActor()
            a.start()
            b.start()
            await a.stop()
            await b.stop()
        asyncio.get_event_loop().run_until_complete(test())
    
    
    def test_hello(self):
        """
        The base actor takes your string and prints it.  Nothing is returned.
        A pass means that nothing faild.
        """
        async def say_hello():
            a = BaseActor()
            b = StringTestActor()
            a.start()
            b.start()
            for _ in range(10):
                message = StringMessage('Hello world!')
                await asyncio.sleep(0.25)
                await a.tell(b, message)
            await stop_actor(a)
            await stop_actor(b)
    
        asyncio.get_event_loop().run_until_complete(say_hello())
    
    
    def test_add(self):
        """
        Test addition in the actor system
        """
        async def test():
            a = BaseActor()
            b = AddTestActor()
            a.start()
            b.start()
            message = AddIntMessage(1)
            res = await a.ask(b, message)
            self.assertEqual(res, 2, "Response not Equals 2 ({})".format(res))
        asyncio.get_event_loop().run_until_complete(test())
    
    
    def test_load_tell(self):
        """
        Create many tell tests.  At least a 100000 actors for my laptop.  Our
        actors in this test mimic a real world scenario.  Actors are paired and
        then calls are made between them asynchronously.  Multiprocessing is
        avoided.
        """
        async def message(a,b,i):
            await a.tell(b, StringMessage("Hello World {}".format(i)))
        
        
        async def test_helper():  
            string_actors = []
            calling_actors = []
            for i in range(0,100000):
                a = StringTestActor()
                a.start()
                b = BaseActor()
                b.start()
                string_actors.append(a)
                calling_actors.append(b)
            
            #create our tests
            connections = []
            for i in range(0,len(calling_actors)):
                connections.append((calling_actors[i], string_actors[i], i))
            
            print("Executing Tell Load Test")
            await asyncio.gather(*[message(connection[0], connection[1], connection[2]) for connection in connections])
            print("Done Executing Tell Load Test")
            
            for i in range(0, len(calling_actors)):
                await calling_actors[i].stop()
                await string_actors[i].stop()
        
        async def test():
            await test_helper()
        
        asyncio.get_event_loop().run_until_complete(test())
        
        print("Load test Complete")
    '''
    
    def test_load_ask(self):
        """
        Create many ask tests.  At least 10000 actors for my laptop.  Our
        actors in this test mimic a real world scenario.  Actors are paired and
        then calls are made between them asynchronously.  Multiprocessing is
        avoided 
        """
        pass

class ActorSystemTest(unittest.TestCase): pass


class RoundRobinRouterTest(unittest.TestCase): pass


class RandomRouterTest(unittest.TestCase): pass


class OnReadyRouterTest(unittest.TestCase): pass


class BalancingRouterTest(unittest.TestCase): pass


class HealthCheckTest(unittest.TestCase): pass


if __name__ == "__main__":
    unittest.main()
