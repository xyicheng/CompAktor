'''
Created on Oct 12, 2017

@author: aevans
'''

import asyncio
import random
from compaktor.routing.round_robin import RoundRobinRouter
from compaktor.system.actor_system import ActorSystem
from compaktor.state.actor_state import ActorState
from compaktor.actor.base_actor import BaseActor


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
    sys.add_actor(a, "test")
    sys.add_actor(b, "test/{}".format(a.get_name()))
    sys.add_actor(d, "test/{}/{}".format(a.get_name(), b.get_name()))
    sys.add_actor(c, "test")
    sys.print_tree()

    sys.close(do_print=False)
    assert(a.get_state() is ActorState.TERMINATED), "Actor was not Terminated"
    assert(b.get_state(), ActorState.TERMINATED), "Actor was not Terminated"
    assert(c.get_state() is ActorState.TERMINATED), "Actor was not Terminated"
    assert(d.get_state() is ActorState.TERMINATED), "Actor was not Terminated"
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
    sys.add_actor(a, "test")
    sys.add_actor(b, "test/{}".format(a.get_name()))
    sys.add_actor(d, "test/{}/{}".format(a.get_name(), b.get_name()))
    sys.add_actor(c, "test")
    sys.print_tree()

    sys.close(do_print=False)
    assert(a.get_state() is ActorState.TERMINATED), "Actor was not Terminated"
    assert(b.get_state() is ActorState.TERMINATED), "Actor was not Terminated"
    assert(c.get_state() is ActorState.TERMINATED), "Actor was not Terminated"
    assert(d.get_state() is ActorState.TERMINATED), "Actor was not Terminated"
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
            # choose random
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
    assert(
        len([x for x in level_one if x.get_state() is ActorState.RUNNING]) is
        0)
    assert(len([x for x in level_one if x.get_state(
    ) is ActorState.TERMINATED]) == len(level_one))
    assert(
        len([x for x in level_two if x.get_state() is ActorState.RUNNING]) is
        0)
    assert(len([x for x in level_two if x.get_state(
    ) is ActorState.TERMINATED]) is len(level_two))


def test_stress_line_of_levels(self):
    print("Testing Long Line of Actors")
    sys = ActorSystem("test")
    path = "test"
    actors = []
    for i in range(1, 10000):
        actor = BaseActor()
        actor.start()
        sys.add_actor(actor, path)
        actors.append(actor)
        path += "/{}".format(actor.get_name())
    print("Closing Actors")
    sys.close()
    assert(
        len([x for x in actors if x.get_state() is ActorState.TERMINATED]) ==
        len(actors))


def test_creation(self):
    print("Create Router Test")
    sys = ActorSystem("tests")
    kwargs = {'name': 'test_router'}
    args = []
    rr = RoundRobinRouter(*args, **kwargs)
    rr.start()
    assert(rr.get_name() == 'test_router')
    rr.set_actor_system(sys, "tests")
    sys.close()
    assert(rr.get_state() == ActorState.TERMINATED), "Router Not Terminated"
    print("Completed Router Creation Test")
