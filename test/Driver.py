'''
Sadly, unit test is not working.  The first test is just ported over from
Cleveland.  Other tests include router tests, remote actor tests, and more.
Some tests may be chained but others cannot be.

Created on Aug 19, 2017

@author: aevans
'''


import asyncio
import gc
from multiprocessing import Process
import random
import socket
import sys
import unittest
from unittest import TestSuite
import aiounittest
from compaktor.actor.actor import BaseActor, ActorState
from compaktor.actor.message import Message, QueryMessage
from compaktor.system.actor_system import ActorSystem
from compaktor.router.routers import RoundRobinRouter
from setuptools.command.easy_install import sys_executable


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
        print("Testing Actor Addition")
        sys = ActorSystem("test")
        a = BaseActor()
        a.start()
        sys.add_actor(a, "test")
        n = sys.get_actor_node("test/1")
        sys.print_tree()
        a = n.actor
        asyncio.get_event_loop().run_until_complete(a.stop())
        sys.close()
        del sys
        gc.collect()
        del gc.garbage[:]
        print("Finished Testing Actor Addition")
    
    
    def test_actor_removal(self):
        """
        Test actor addition and then removal
        """
        print("Testing Actor Removal")
        sys = ActorSystem("test")
        a = BaseActor()
        a.start()
        sys.add_actor(a, "test")
        sys.delete_branch("1")
        self.assertEqual(a.get_state(), ActorState.TERMINATED, "Actor was not Terminated")
        print("Printing Tree")
        sys.print_tree()
        asyncio.get_event_loop().run_until_complete(a.stop())
        sys.close()
        del sys
        gc.collect()
        del gc.garbage[:]
        print("Finished Testing Actor Removal")
    
    
    def test_branch_creation(self):
        """
        Test branch creation
        """
        print("Testing Branch Creation")
        a = BaseActor()
        a.start()
        b = BaseActor()
        b.start()
        c = BaseActor()
        c.start()
        d = BaseActor()
        d.start()
        
        sys = ActorSystem("test")
        sys.add_actor(a,"test")
        sys.add_actor(b, "test/{}".format(a.get_name()))
        sys.add_actor(d, "test/{}/{}".format(a.get_name(),b.get_name()))
        sys.add_actor(c,"test")
        sys.print_tree()
        
        sys.close(do_print = False)
        self.assertEqual(a.get_state(), ActorState.TERMINATED, "Actor was not Terminated")
        self.assertEqual(b.get_state(), ActorState.TERMINATED, "Actor was not Terminated")
        self.assertEqual(c.get_state(), ActorState.TERMINATED, "Actor was not Terminated")
        self.assertEqual(d.get_state(), ActorState.TERMINATED, "Actor was not Terminated")
        print("Finished Testing Branch Creation")
    
    
    def test_branch_removal(self):
        """
        Add a basic branch and remove it.
        """
        print("Testing Branch Creation")
        a = BaseActor()
        a.start()
        b = BaseActor()
        b.start()
        c = BaseActor()
        c.start()
        d = BaseActor()
        d.start()
        
        sys = ActorSystem("test")
        sys.add_actor(a,"test")
        sys.add_actor(b, "test/{}".format(a.get_name()))
        sys.add_actor(d, "test/{}/{}".format(a.get_name(),b.get_name()))
        sys.add_actor(c,"test")
        sys.print_tree()
        
        sys.close(do_print = False)
        self.assertEqual(a.get_state(), ActorState.TERMINATED, "Actor was not Terminated")
        self.assertEqual(b.get_state(), ActorState.TERMINATED, "Actor was not Terminated")
        self.assertEqual(c.get_state(), ActorState.TERMINATED, "Actor was not Terminated")
        self.assertEqual(d.get_state(), ActorState.TERMINATED, "Actor was not Terminated")
        print("Finished Testing Branch Creation")

    
    def test_stress_add_two_levels(self):
        """
        Test actors added to two levels of the tree.  150000 actors added.
        """
        level_one = []
        level_two = []
        sys = ActorSystem("test")
        async def test():
            print("Creating Actors")
            for i in range(1, 50000):
                a = BaseActor()
                a.start()
                sys.add_actor(a, "test")
                level_one.append(a)
    
            for i in range(1, 100000):
                #choose random
                a = BaseActor()
                a.start()
                parent = random.choice(level_one)
                path = "test/{}".format(parent.get_name().strip())
                sys.add_actor(a, path)
                level_two.append(parent)
            
        asyncio.get_event_loop().run_until_complete(test())
        print("Closing Actors")
        sys.close()
        print("Finished Closing System")
        self.assertEqual(len([x for x in level_one if x.get_state() is ActorState.RUNNING ]), 0 )
        self.assertEqual(len([x for x in level_one if x.get_state() is ActorState.TERMINATED]), len(level_one))
        self.assertEqual(len([x for x in level_two if x.get_state() is ActorState.RUNNING ]), 0 )
        self.assertEqual(len([x for x in level_two if x.get_state() is ActorState.TERMINATED]), len(level_two))

        
    def test_stress_line_of_levels(self):
        print("Testing Long Line of Actors")
        sys = ActorSystem("test")
        path = "test"
        actors = []
        for i in range(1,10000):
            actor = BaseActor()
            actor.start()
            sys.add_actor(actor, path)
            actors.append(actor)
            path += "/{}".format(actor.get_name())
        print("Closing Actors")
        sys.close()
        self.assertEqual(len([x for x in actors if x.get_state() is ActorState.TERMINATED]), len(actors))
    
    def runTest(self):
        self.test_actor_addition()
        self.test_actor_removal()
        self.test_branch_creation()
        self.test_branch_removal()
        self.test_stress_add_two_levels()
        self.test_stress_line_of_levels()
    
    
class RoundRobinRouterTest(unittest.TestCase):
    
    
    def test_creation(self):
        print("Create Router Test")
        sys = ActorSystem("tests")
        kwargs = {'name' : 'test_router'}
        args = []
        rr = RoundRobinRouter(*args, **kwargs)
        rr.start()
        self.assertEqual(rr.get_name(), 'test_router')
        rr.set_actor_system(sys, "tests")
        sys.close()
        self.assertEqual(rr.get_state(), ActorState.TERMINATED, "Router Not Terminated")
        print("Completed Router Creation Test")
    
    def test_actor_addition(self):
        print("Starting Actor Addition Test")
        sys = ActorSystem("tests")
        kwargs = {'name' : 'test_router'}
        args = []
        rr = RoundRobinRouter(*args, **kwargs)
        rr.start()
        rr.set_actor_system(sys, "tests")
        
        a = BaseActor()
        a.start()
        rr.add_actor(a)
        
        b = BaseActor()
        b.start()
        rr.add_actor(b)
        
        self.assertEqual(rr.get_num_actors(), 2, "Actors Missing. Length {}".format(rr.get_num_actors()))
        
        rr.remove_actor(a)
        self.assertEqual(rr.get_num_actors(), 1, "Number of Actors Should be 1")
        asyncio.get_event_loop().run_until_complete(a.stop())
        sys.close()
        self.assertEqual(a.get_state(), ActorState.TERMINATED, "Actor a Not Terminated")
        self.assertEqual(b.get_state(), ActorState.TERMINATED, " Actor b Not Terminated")
        self.assertEqual(rr.get_state(), ActorState.TERMINATED, "Router Not Terminated")
        print("Actor Addition Test Complete")
    
    
    def test_multiplication(self):
        print("Testing multiplication")
        sys = ActorSystem("tests")
        kwargs = {'name' : 'test_router'}
        args = []
        rr = RoundRobinRouter(*args, **kwargs)
        rr.start()
        rr.set_actor_system(sys, "tests")
        
        a = AddTestActor()
        a.start()
        rr.add_actor(a)
        
        b = AddTestActor()
        b.start()
        rr.add_actor(b)
        
        async def get_addition():
            res = await rr.route_ask(AddIntMessage(1))
            return res
        
        res = asyncio.get_event_loop().run_until_complete(get_addition())
        print(rr.get_current_index())
        self.assertEqual(res, 2, "Addition Not Completed")
        
        self.assertEqual(rr.get_num_actors(), 2, "Actors Missing. Length {}".format(rr.get_num_actors()))
        sys.close()
        
        self.assertEqual(a.get_state(), ActorState.TERMINATED, "Actor a Not Terminated")
        self.assertEqual(b.get_state(), ActorState.TERMINATED, " Actor b Not Terminated")
        self.assertEqual(rr.get_state(), ActorState.TERMINATED, "Router Not Terminated")
        print("Done Testing Multiplication")    
    
    
    def test_tell(self):
        print("Starting Tell Test")
        sys = ActorSystem("tests")
        kwargs = {'name' : 'test_router'}
        args = []
        rr = RoundRobinRouter(*args, **kwargs)
        rr.start()
        rr.set_actor_system(sys, "tests")
        
        a = StringTestActor()
        a.start()
        rr.add_actor(a)
        
        b = StringTestActor()
        b.start()
        rr.add_actor(b)
        
        self.assertEqual(rr.get_num_actors(), 2, "Actors Missing. Length {}".format(rr.get_num_actors()))
        
        asyncio.get_event_loop().run_until_complete(rr.route_tell(StringMessage("Hello World")))
        
        sys.close()
        self.assertEqual(a.get_state(), ActorState.TERMINATED, "Actor a Not Terminated")
        self.assertEqual(b.get_state(), ActorState.TERMINATED, " Actor b Not Terminated")
        self.assertEqual(rr.get_state(), ActorState.TERMINATED, "Router Not Terminated")
        print("Tell Test Complete")
    
    
    def test_broadcast(self):
        print("Testing Broadcast")
        sys = ActorSystem("tests")
        kwargs = {'name' : 'test_router'}
        args = []
        rr = RoundRobinRouter(*args, **kwargs)
        rr.start()
        rr.set_actor_system(sys, "tests")
        
        a = StringTestActor()
        a.start()
        rr.add_actor(a)
        
        b = StringTestActor()
        b.start()
        rr.add_actor(b)
        
        asyncio.get_event_loop().run_until_complete(rr.broadcast(StringMessage("Hello World!")))
        self.assertEqual(rr.get_num_actors(), 2, "Actors Missing. Length {}".format(rr.get_num_actors()))
        
        asyncio.get_event_loop().run_until_complete(rr.route_tell(StringMessage("Hello World")))
        
        sys.close()
        self.assertEqual(a.get_state(), ActorState.TERMINATED, "Actor a Not Terminated")
        self.assertEqual(b.get_state(), ActorState.TERMINATED, " Actor b Not Terminated")
        self.assertEqual(rr.get_state(), ActorState.TERMINATED, "Router Not Terminated")
        print("Finished Testing Broadcast")
    
    
    def test_at_load(self):
        print("Load Testing")
        sys = ActorSystem("tests")
        kwargs = {'name' : 'test_router'}
        args = []
        rr = RoundRobinRouter(*args, **kwargs)
        rr.start()
        rr.set_actor_system(sys, "tests")
        
        actors = []
        
        async def get_addition():
            res = await rr.route_ask(AddIntMessage(1))
            return res
        
        async def get_sum(funcs):
            res = await asyncio.gather(*funcs)
            return sum(res) / 2
        
        print("Starting Actor")
        for i in range(0,50000):
            a = AddTestActor()
            a.start()
            rr.add_actor(a)
            actors.append(a)
        
        print("Start Adding")
        funcs = [get_addition() for x in actors]
        print("Waiting")
        res = asyncio.get_event_loop().run_until_complete(get_sum(funcs))
        print(res)
        self.assertEqual(res, len(funcs), "Result {} is not {}".format(res, len(funcs)))
        sys.close()
        self.assertEqual(rr.get_state(), ActorState.TERMINATED, "Router Not Terminated")
        print("Done Load Testing")
    
    
    def test_tell_at_load(self):
        print("Load Testing With Tell")
        sys = ActorSystem("tests")
        kwargs = {'name' : 'test_router'}
        args = []
        rr = RoundRobinRouter(*args, **kwargs)
        rr.start()
        rr.set_actor_system(sys, "tests")
        
        actors = []
        
        async def say_hello(i):
            await rr.route_tell(StringMessage("Hello {}".format(i)))
        
        async def comp(funcs):
            await asyncio.gather(*funcs)
        
        print("Starting Actor")
        for i in range(0,20000):
            a = StringTestActor()
            a.start()
            rr.add_actor(a)
            actors.append(a)
        
        print("Start Adding")
        funcs = [say_hello(i) for i in range(0,len(actors))]
        print("Waiting")
        asyncio.get_event_loop().run_until_complete(comp(funcs))
        sys.close()
        del sys
        gc.collect()
        del gc.garbage[:]
        self.assertEqual(rr.get_state(), ActorState.TERMINATED, "Router Not Terminated")
        print("Load Testing With Tell Complete")

    def runTest(self):
        self.test_creation()
        self.test_actor_addition()
        self.test_tell()
        
        self.test_multiplication()
        self.test_broadcast()
        self.test_at_load()
        
        self.test_tell_at_load()
        self.test_mixed_load()
        

class RandomRouterTest(unittest.TestCase): 


    def test_creation(self):
        print("Create Router Test")
        sys = ActorSystem("tests")
        kwargs = {'name' : 'test_router'}
        args = []
        rr = RoundRobinRouter(*args, **kwargs)
        rr.start()
        self.assertEqual(rr.get_name(), 'test_router')
        rr.set_actor_system(sys, "tests")
        sys.close()
        self.assertEqual(rr.get_state(), ActorState.TERMINATED, "Router Not Terminated")
        print("Completed Router Creation Test")
    
    def test_actor_addition(self):
        print("Starting Actor Addition Test")
        sys = ActorSystem("tests")
        kwargs = {'name' : 'test_router'}
        args = []
        rr = RoundRobinRouter(*args, **kwargs)
        rr.start()
        rr.set_actor_system(sys, "tests")
        
        a = BaseActor()
        a.start()
        rr.add_actor(a)
        
        b = BaseActor()
        b.start()
        rr.add_actor(b)
        
        self.assertEqual(rr.get_num_actors(), 2, "Actors Missing. Length {}".format(rr.get_num_actors()))
        
        rr.remove_actor(a)
        self.assertEqual(rr.get_num_actors(), 1, "Number of Actors Should be 1")
        asyncio.get_event_loop().run_until_complete(a.stop())
        sys.close()
        self.assertEqual(a.get_state(), ActorState.TERMINATED, "Actor a Not Terminated")
        self.assertEqual(b.get_state(), ActorState.TERMINATED, " Actor b Not Terminated")
        self.assertEqual(rr.get_state(), ActorState.TERMINATED, "Router Not Terminated")
        print("Actor Addition Test Complete")
    
    
    def test_multiplication(self):
        print("Testing multiplication")
        sys = ActorSystem("tests")
        kwargs = {'name' : 'test_router'}
        args = []
        rr = RoundRobinRouter(*args, **kwargs)
        rr.start()
        rr.set_actor_system(sys, "tests")
        
        a = AddTestActor()
        a.start()
        rr.add_actor(a)
        
        b = AddTestActor()
        b.start()
        rr.add_actor(b)
        
        async def get_addition():
            res = await rr.route_ask(AddIntMessage(1))
            return res
        
        res = asyncio.get_event_loop().run_until_complete(get_addition())
        print(rr.get_current_index())
        self.assertEqual(res, 2, "Addition Not Completed")
        
        self.assertEqual(rr.get_num_actors(), 2, "Actors Missing. Length {}".format(rr.get_num_actors()))
        sys.close()
        
        self.assertEqual(a.get_state(), ActorState.TERMINATED, "Actor a Not Terminated")
        self.assertEqual(b.get_state(), ActorState.TERMINATED, " Actor b Not Terminated")
        self.assertEqual(rr.get_state(), ActorState.TERMINATED, "Router Not Terminated")
        print("Done Testing Multiplication")    
    
    
    def test_tell(self):
        print("Starting Tell Test")
        sys = ActorSystem("tests")
        kwargs = {'name' : 'test_router'}
        args = []
        rr = RoundRobinRouter(*args, **kwargs)
        rr.start()
        rr.set_actor_system(sys, "tests")
        
        a = StringTestActor()
        a.start()
        rr.add_actor(a)
        
        b = StringTestActor()
        b.start()
        rr.add_actor(b)
        
        self.assertEqual(rr.get_num_actors(), 2, "Actors Missing. Length {}".format(rr.get_num_actors()))
        
        asyncio.get_event_loop().run_until_complete(rr.route_tell(StringMessage("Hello World")))
        
        sys.close()
        self.assertEqual(a.get_state(), ActorState.TERMINATED, "Actor a Not Terminated")
        self.assertEqual(b.get_state(), ActorState.TERMINATED, " Actor b Not Terminated")
        self.assertEqual(rr.get_state(), ActorState.TERMINATED, "Router Not Terminated")
        print("Tell Test Complete")
    
    
    def test_broadcast(self):
        print("Testing Broadcast")
        sys = ActorSystem("tests")
        kwargs = {'name' : 'test_router'}
        args = []
        rr = RoundRobinRouter(*args, **kwargs)
        rr.start()
        rr.set_actor_system(sys, "tests")
        
        a = StringTestActor()
        a.start()
        rr.add_actor(a)
        
        b = StringTestActor()
        b.start()
        rr.add_actor(b)
        
        asyncio.get_event_loop().run_until_complete(rr.broadcast(StringMessage("Hello World!")))
        self.assertEqual(rr.get_num_actors(), 2, "Actors Missing. Length {}".format(rr.get_num_actors()))
        
        asyncio.get_event_loop().run_until_complete(rr.route_tell(StringMessage("Hello World")))
        
        sys.close()
        self.assertEqual(a.get_state(), ActorState.TERMINATED, "Actor a Not Terminated")
        self.assertEqual(b.get_state(), ActorState.TERMINATED, " Actor b Not Terminated")
        self.assertEqual(rr.get_state(), ActorState.TERMINATED, "Router Not Terminated")
        print("Finished Testing Broadcast")
    
    
    def test_at_load(self):
        print("Load Testing")
        sys = ActorSystem("tests")
        kwargs = {'name' : 'test_router'}
        args = []
        rr = RoundRobinRouter(*args, **kwargs)
        rr.start()
        rr.set_actor_system(sys, "tests")
        
        actors = []
        
        async def get_addition():
            res = await rr.route_ask(AddIntMessage(1))
            return res
        
        async def get_sum(funcs):
            res = await asyncio.gather(*funcs)
            return sum(res) / 2
        
        print("Starting Actor")
        for i in range(0,50000):
            a = AddTestActor()
            a.start()
            rr.add_actor(a)
            actors.append(a)
        
        print("Start Adding")
        funcs = [get_addition() for x in actors]
        print("Waiting")
        res = asyncio.get_event_loop().run_until_complete(get_sum(funcs))
        print(res)
        self.assertEqual(res, len(funcs), "Result {} is not {}".format(res, len(funcs)))
        sys.close()
        self.assertEqual(rr.get_state(), ActorState.TERMINATED, "Router Not Terminated")
        print("Done Load Testing")
    
    
    def test_tell_at_load(self):
        print("Load Testing With Tell")
        sys = ActorSystem("tests")
        kwargs = {'name' : 'test_router'}
        args = []
        rr = RoundRobinRouter(*args, **kwargs)
        rr.start()
        rr.set_actor_system(sys, "tests")
        
        actors = []
        
        async def say_hello(i):
            await rr.route_tell(StringMessage("Hello {}".format(i)))
        
        async def comp(funcs):
            await asyncio.gather(*funcs)
        
        print("Starting Actor")
        for i in range(0,20000):
            a = StringTestActor()
            a.start()
            rr.add_actor(a)
            actors.append(a)
        
        print("Start Adding")
        funcs = [say_hello(i) for i in range(0,len(actors))]
        print("Waiting")
        asyncio.get_event_loop().run_until_complete(comp(funcs))
        sys.close()
        del sys
        gc.collect()
        del gc.garbage[:]
        self.assertEqual(rr.get_state(), ActorState.TERMINATED, "Router Not Terminated")
        print("Load Testing With Tell Complete")


    def runTest(self):
        self.test_creation()
        #self.test_actor_addition()
        #self.test_tell()
        
        #self.test_multiplication()
        #self.test_broadcast()
        #self.test_at_load()
        
        #self.test_tell_at_load()
        #self.test_mixed_load()


class BalancingRouterTest(unittest.TestCase):
    
    
    def test_creation(self):
        print("Starting Creation Test")
        
        print("Completed Creation Test")
    
    def test_actor_addition(self):
        print("Starting Actor Addition Test")
        
        print("Actor Addition Test Complete")
    
    
    def test_multiplication(self):
        print("Testing multiplication")
        
        print("Done Testing Multiplication")    
    

    def test_at_load(self):
        print("Load Testing")
        
        print("Done Load Testing")
    
    def runTest(self):
        self.test_creation()
        self.test_actor_addition()
        self.test_multiplication()
        self.test_at_load()


class HealthCheckTest(unittest.TestCase): 


    def test_heartbeat(self):
        print("Started Heartbeat Test")
        
        print("Completed Heartbeat Test")
    
    
    def test_failure(self):
        print("Started Failure Test")
        
        print("Completed Failure Test")


def suite():
    suite = TestSuite()
    #suite.addTest(ActorTest())
    #suite.addTest(ActorSystemTest())
    #suite.addTest(RoundRobinRouterTest())
    suite.addTest(RandomRouterTest())
    return suite


if __name__ == "__main__":
    sys.setrecursionlimit(0x5000000)
    runner = unittest.TextTestRunner()
    test_suite = suite()
    runner.run (test_suite)
