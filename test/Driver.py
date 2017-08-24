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
from unittest import TestSuite
import aiounittest
from compaktor.actor.actor import BaseActor, ActorState
from compaktor.actor.message import Message, QueryMessage
from compaktor.system.actor_system import ActorSystem


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
    
    
    def test_load_ask(self):
        """
        Create many ask tests.  At least 10000 actors for my laptop.  Our
        actors in this test mimic a real world scenario.  Actors are paired and
        then calls are made between them asynchronously.  Multiprocessing is
        avoided 
        """
        num_actors = 100000
        async def message(a,b):
            return await a.ask(b, AddIntMessage(1))
        
        
        async def test_helper():  
            string_actors = []
            calling_actors = []
            for i in range(0,num_actors):
                a = AddTestActor()
                a.start()
                b = BaseActor()
                b.start()
                string_actors.append(a)
                calling_actors.append(b)
            
            #create our tests
            connections = []
            for i in range(0,len(calling_actors)):
                connections.append((calling_actors[i], string_actors[i], i))
            
            print("Executing Ask Load Test")
            results = await asyncio.gather(*[message(connection[0], connection[1]) for connection in connections])
            print("Done Executing Tell Load Test")
            
            print("Stopping Ask Actors")
            for i in range(0, len(calling_actors)):
                await calling_actors[i].stop()
                await string_actors[i].stop()
                
            print("Checking Sum")
            self.assertEqual(sum(results) / 2, num_actors)
        
        async def test():
            await test_helper()
        
        asyncio.get_event_loop().run_until_complete(test())
        
        
        def runTest(self):
            self.test_add()
            self.test_hello()
            self.test_load_ask()
            self.test_load_tell()
            self.test_serialization()
            self.test_setup()


class ActorSystemTest(unittest.TestCase): 
    
    
    def test_empty_setup(self):
        """
        Instantiate and call close to ensure the system shuts down
        """
        sys = ActorSystem()
        sys.close()
    
    
    def test_actor_addition(self):
        """
        Instantiate add a single actor and call close.  The final actors state
        should be TERMINATED.  The number of remaining children should be 0.
        """
        sys = ActorSystem("test")
        a = StringTestActor()
        sys.add_actor(a,  None)
        sys.close()
        print("closed")
        print(a.get_state())
        self.assertEqual(a.get_state(), ActorState.TERMINATED, "Actor was not Terminated")
        self.assertEqual(sys.is_empty(), True, "Actor System contains Actors")
    
    
    def test_actor_removal(self):
        pass
    
    
    def test_branch_creation(self):
        pass
    
    
    def test_branch_removal(self):
        pass

    
    def test_stress_add(self):
        pass
    
    
    def test_stress_add_remove(self):
        pass
    
    
    def test_addition_with_branch(self):
        pass
    
    
    def runTest(self):
        self.test_actor_addition()
        self.test_actor_removal()
        self.test_branch_creation()
        self.test_branch_removal()
        self.test_stress_add()
        self.test_stress_add_remove()
        self.test_addition_with_branch()
    
    
class RoundRobinRouterTest(unittest.TestCase):
    
    
    def test_creation(self):
        pass
    
    
    def test_addition(self):
        pass
    
    
    def test_removal(self):
        pass
    
    
    def test_multi_addition(self):
        pass


    def test_at_load(self):
        pass
    
    
    def test_failure(self):
        pass
    
    
    def runTest(self):
        self.test_creation()
        self.test_addition()
        self.test_removal()
        self.test_multi_addition()
        self.test_at_load()


class RandomRouterTest(unittest.TestCase): 


    def test_creation(self):
        pass
    
    
    def test_addition(self):
        pass
    
    
    def test_removal(self):
        pass
    
    
    def test_multi_addition(self):
        pass


    def test_at_load(self):
        pass
    
    
    def test_failure(self):
        pass
    
    
    def runTest(self):
        self.test_creation()
        self.test_addition()
        self.test_removal()
        self.test_multi_addition()
        self.test_at_load()


class BalancingRouterTest(unittest.TestCase):
    
    
    def test_creation(self):
        pass
    
    
    def test_addition(self):
        pass
    
    
    def test_removal(self):
        pass
    
    
    def test_multi_addition(self):
        pass


    def test_at_load(self):
        pass
    
    
    def test_failure(self):
        pass
    
    
    def runTest(self):
        self.test_creation()
        self.test_addition()
        self.test_removal()
        self.test_multi_addition()
        self.test_at_load()


class HealthCheckTest(unittest.TestCase): 


    def test_heartbeat(self):
        pass
    
    
    def test_failure(self):
        pass


def suite():
    suite = TestSuite()
    suite.addTest(ActorSystemTest())
    return suite


if __name__ == "__main__":
    runner = unittest.TextTestRunner()
    test_suite = suite()
    runner.run (test_suite)
