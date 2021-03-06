'''
Created on Oct 24, 2017

@author: aevans
'''

import asyncio
from test.modules.actors import AddTestActor, AddIntMessage, StringMessage,\
                                StringTestActor, ObjectTestActor, ObjectMessage
from compaktor.actor.base_actor import BaseActor
import unittest


class TestActors(unittest.TestCase):

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
        A pass means that nothing failed.
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
            await a.stop()
            await b.stop()

        asyncio.get_event_loop().run_until_complete(say_hello())

    def test_add(self):
        """
        Test addition in the actor system
        """
        async def test():
            a = BaseActor()
            b = AddTestActor("test")
            a.start()
            b.start()
            message = AddIntMessage(1)
            res = await a.ask(b, message)
            assert(res == 2), "Response not Equals 2 ({})".format(res)
        asyncio.get_event_loop().run_until_complete(test())

    def test_load_tell(self):
        """
        Create many tell tests.  At least a 100000 actors for my laptop.  Our
        actors in this test mimic a real world scenario.  Actors are paired and
        then calls are made between them asynchronously.  Multiprocessing is
        avoided.
        """
        async def message(a, b, i):
            await a.tell(b, StringMessage("Hello World {}".format(i)))

        async def test_helper():
            string_actors = []
            calling_actors = []
            for i in range(0, 1000):
                a = StringTestActor()
                a.start()
                b = BaseActor()
                b.start()
                string_actors.append(a)
                calling_actors.append(b)

            # create our tests
            connections = []
            for i in range(0, len(calling_actors)):
                connections.append((calling_actors[i], string_actors[i], i))

            print("Executing Tell Load Test")
            await asyncio.gather(
                *[
                    message(
                        connection[0],
                        connection[1],
                        connection[2]) for connection in connections])
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
        avoided.
        """
        num_actors = 1000

        async def message(a, b):
            return await a.ask(b, AddIntMessage(1))

        async def test_helper():
            string_actors = []
            calling_actors = []
            for i in range(0, num_actors):
                a = AddTestActor("testa")
                a.start()
                b = BaseActor("testb")
                b.start()
                string_actors.append(a)
                calling_actors.append(b)

            # create our tests
            connections = []
            for i in range(0, len(calling_actors)):
                connections.append((calling_actors[i], string_actors[i], i))

            print("Executing Ask Load Test")
            results = await asyncio.gather(
                *[
                    message(
                        connection[0],
                        connection[1]) for connection in connections])
            print("Done Executing Tell Load Test")

            print("Stopping Ask Actors")
            for i in range(0, len(calling_actors)):
                await calling_actors[i].stop()
                await string_actors[i].stop()

            print("Checking Sum")
            sm = sum(results)
            print(sm)
            assert(sm / 2 == num_actors)

        async def test():
            await test_helper()
        asyncio.get_event_loop().run_until_complete(test())

    def test_multi_loop(self):
        print("Testing Actors on Multiple Loops")
        add_loop = asyncio.new_event_loop()
        a = AddTestActor("testa", loop=add_loop)
        a.start()
        base_loop = asyncio.new_event_loop()
        b = BaseActor("testb", loop=base_loop)
        b.start()
        print("Telling")
        asyncio.get_event_loop().run_until_complete(b.tell(a, AddIntMessage(1, b)))
        print("Asking")
        res = asyncio.get_event_loop().run_until_complete(b.ask(a, AddIntMessage(1,b)))
        print("Complete")
        assert(res == 2)

if __name__ == "__main__":
    unittest.main()
