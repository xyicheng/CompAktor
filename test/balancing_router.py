'''


Created on Oct 12, 2017

@author: aevans
'''

import asyncio
from test.modules.actors import AddTestActor, AddIntMessage, StringMessage,\
                                StringTestActor
from compaktor.system.actor_system import ActorSystem
from compaktor.state.actor_state import ActorState
from compaktor.routing.balancing import BalancingRouter
from compaktor.actor.base_actor import BaseActor
from compaktor.message.message_objects import RouteTell, RouteAsk


def test_balancing_router_creation():
    print("Starting Creation Test")
    sys = ActorSystem("test")
    rr = BalancingRouter("test_router")
    rr.start()
    sys.add_actor(rr, "test")
    sys.close()
    assert(rr.get_state(), ActorState.TERMINATED), "Router Not Closed"
    print("Completed Creation Test")


def test_balancing_router_actor_addition():
    print("Starting Actor Addition Test")
    sys = ActorSystem("test")
    rr = BalancingRouter("test_router")
    rr.start()
    print(rr.get_name())
    a = BaseActor()
    a.start()
    rr.add_actor(a)
    sys.add_actor(rr, "test")
    rr.remove_actor(a)
    assert(rr.get_num_actors() is 0), "Actor Still in Router"
    sys.close()
    asyncio.get_event_loop().run_until_complete(a.stop())
    assert(rr.get_state() is ActorState.TERMINATED), "Router Not Closed"
    assert(a.get_state() is ActorState.TERMINATED), "Actor is Not Closed"
    print("Actor Addition Test Complete")


def test_balancing_router_arithemetic():
    print("Testing multiplication")
    sys = ActorSystem("tests")
    rr = BalancingRouter("test_router")
    rr.start()
    rr.set_actor_system(sys, "tests")

    a = AddTestActor(name="testa", inbox=rr.get_router_queue())
    a.start()
    rr.add_actor(a)

    b = AddTestActor(name="testb", inbox=rr.get_router_queue())
    b.start()
    rr.add_actor(b)
    res = asyncio.get_event_loop().run_until_complete(rr.route_ask(RouteAsk(AddIntMessage(1))))
    assert(res is 2), "Addition Not Completed"
    msg = "Actors Missing. Length {}".format(rr.get_num_actors())
    assert(rr.get_num_actors() is 2), msg
    rr.close_queue()
    asyncio.get_event_loop().run_until_complete(a.stop())
    asyncio.get_event_loop().run_until_complete(b.stop())
    asyncio.get_event_loop().run_until_complete(rr.stop())
    assert(a.get_state() is ActorState.TERMINATED), "Actor a Not Terminated"
    assert(b.get_state() is ActorState.TERMINATED), " Actor b Not Terminated"
    assert(rr.get_state() is ActorState.TERMINATED), "Router Not Terminated"
    print("Done Testing Multiplication")


def test_balancing_at_load():
    print("Load Testing")
    sys = ActorSystem("tests")
    rr = BalancingRouter("test_router")
    rr.start()
    rr.set_actor_system(sys, "tests")

    actors = []

    async def say_hello(i):
        await rr.route_tell(StringMessage("Hello {}".format(i)))

    async def comp(funcs):
        await asyncio.gather(*funcs)

    print("Starting Actor")
    for i in range(0, 10):
        a = StringTestActor()
        a.start()
        rr.add_actor(a)
        actors.append(a)

    print("Start Adding")
    funcs = [say_hello(i) for i in range(0, len(actors))]
    print("Waiting")
    asyncio.get_event_loop().run_until_complete(comp(funcs))
    print("Complete")
    sys.print_tree()
    sys.close()
    rr.close_queue()
    sys.print_tree()
    print("Garbage Collecting")
    assert(rr.get_state() is ActorState.TERMINATED), "Router Not Terminated"
    print("Done Load Testing")


if __name__ == "__main__":
    test_balancing_router_arithemetic()
