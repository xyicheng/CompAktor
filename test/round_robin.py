
import asyncio
import gc
from test.modules.actors import AddTestActor, AddIntMessage, StringMessage,\
                                StringTestActor
from compaktor.system.actor_system import ActorSystem
from compaktor.state.actor_state import ActorState
from compaktor.routing.round_robin import RoundRobinRouter
from compaktor.actor.base_actor import BaseActor


def test_round_robin_actor_addition(self):
    print("Starting Actor Addition Test")
    sys = ActorSystem("tests")
    kwargs = {'name': 'test_router'}
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
    msg = "Actors Missing. Length {}".format(rr.get_num_actors())
    assert(rr.get_num_actors() == 2), msg
    rr.remove_actor(a)
    assert(rr.get_num_actors() == 1), "Number of Actors Should be 1"
    asyncio.get_event_loop().run_until_complete(a.stop())
    sys.close()
    assert(a.get_state() == ActorState.TERMINATED), "Actor a Not Terminated"
    assert(b.get_state() == ActorState.TERMINATED), " Actor b Not Terminated"
    assert(rr.get_state() == ActorState.TERMINATED), "Router Not Terminated"
    print("Actor Addition Test Complete")


def test_round_robin_multiplication(self):
    print("Testing multiplication")
    a_sys = ActorSystem("tests")
    kwargs = {'name': 'test_router'}
    args = []
    rr = RoundRobinRouter(*args, **kwargs)
    rr.start()
    rr.set_actor_system(a_sys, "tests")

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
    assert(res is 2), "Addition Not Completed"
    msg = "Actors Missing. Length {}".format(rr.get_num_actors())
    assert(rr.get_num_actors() is 2), msg
    a_sys.close()

    assert(a.get_state() is ActorState.TERMINATED), "Actor a Not Terminated"
    assert(b.get_state(), ActorState.TERMINATED)," Actor b Not Terminated"
    assert(rr.get_state(), ActorState.TERMINATED), "Router Not Terminated"
    print("Done Testing Multiplication")


def test_round_robin_tell(self):
    print("Starting Tell Test")
    sys = ActorSystem("tests")
    kwargs = {'name': 'test_router'}
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
    msg = "Actors Missing. Length {}".format(rr.get_num_actors())
    assert(rr.get_num_actors() == 2), msg

    asyncio.get_event_loop().run_until_complete(
        rr.route_tell(StringMessage("Hello World")))

    sys.close()
    assert(a.get_state() is ActorState.TERMINATED), "Actor a Not Terminated"
    assert(b.get_state() is ActorState.TERMINATED), " Actor b Not Terminated"
    assert(rr.get_state() is ActorState.TERMINATED), "Router Not Terminated"
    print("Tell Test Complete")

def test_round_robin_broadcast(self):
    print("Testing Broadcast")
    sys = ActorSystem("tests")
    kwargs = {'name': 'test_router'}
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

    asyncio.get_event_loop().run_until_complete(
        rr.broadcast(StringMessage("Hello World!")))
    msg = "Actors Missing. Length {}".format(rr.get_num_actors())
    assert(rr.get_num_actors() is 2), msg

    asyncio.get_event_loop().run_until_complete(
        rr.route_tell(StringMessage("Hello World")))

    assert(a.get_state() is ActorState.TERMINATED), "Actor a Not Terminated"
    assert(b.get_state() is ActorState.TERMINATED), " Actor b Not Terminated"
    assert(rr.get_state() is ActorState.TERMINATED), "Router Not Terminated"
    print("Finished Testing Broadcast")

def test_round_robin_at_load(self):
    print("Load Testing")
    sys = ActorSystem("tests")
    kwargs = {'name': 'test_router'}
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
    for i in range(0, 100):
        a = AddTestActor()
        a.start()
        rr.add_actor(a)
        actors.append(a)

    print("Start Adding")
    funcs = [get_addition() for x in actors]
    print("Waiting")
    res = asyncio.get_event_loop().run_until_complete(get_sum(funcs))
    print(res)
    assert(res is len(funcs)), "Result {} is not {}".format(res, len(funcs))
    sys.close()
    assert(rr.get_state() is ActorState.TERMINATED), "Router Not Terminated"
    print("Done Load Testing")

def test__round_robin_tell_at_load(self):
    print("Load Testing With Tell")
    sys = ActorSystem("tests")
    kwargs = {'name': 'test_router'}
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
    for i in range(0, 100):
        a = StringTestActor()
        a.start()
        rr.add_actor(a)
        actors.append(a)

    print("Start Adding")
    funcs = [say_hello(i) for i in range(0, len(actors))]
    print("Waiting")
    asyncio.get_event_loop().run_until_complete(comp(funcs))
    sys.close()
    del sys
    gc.collect()
    del gc.garbage[:]
    assert(rr.get_state() is ActorState.TERMINATED), "Router Not Terminated"
    print("Load Testing With Tell Complete")
