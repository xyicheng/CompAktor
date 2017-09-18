'''
Tests multi-threaded event loops.

Created on Sep 18, 2017

@author: aevans
'''


import time
import asyncio
from compaktor.multithreading.loop_pool import LoopThreadPool
from compaktor.actor.actor import BaseActor
from compaktor.actor.message import Message


class TestMessage(Message):
    pass


class TestActor(BaseActor):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.register_handler(TestMessage, self.test_func)

    async def test_func(self, message):
        print(message)


def test_pool_creation():
    pool = LoopThreadPool()
    assert pool.get_num_threads() is 0


def test_loop_removal():
    pool = LoopThreadPool()
    loop = pool.create_loop()
    assert loop.is_running is True, "Loop Never Started"
    pool.remove(loop)
    assert pool.get_num_threads is 0, "Loop Never Removed from Pool"
    while loop.is_running():
        time.sleep(2)
        print(loop)
    assert loop.is_running is False, "Loop Never Shut Down"


def test_multi_loops():
    pool = LoopThreadPool()
    loops = []
    for i in range(0,3):
        loops.append(pool.create_loop())
    for loop in loops:
        assert loop.is_running is True, "Loop Not Started"
    assert pool.get_num_threads is 3, "3 Loops not Created"
    pool.shutdown()
    assert pool.get_num_threads() is 0, "Loops not Removed"
    for loop in loops:
        assert loop.is_running() is False, "Loop Still Running"


def do_tell(actor, loop):
    asyncio.run_coroutine_threadsafe(actor.tell(actor, TestMessage("Hi There")), loop=loop)


def test_single_actor_with_new_loop():
    pass


def test_multi_loop_thread_at_load():
    pass

    
if __name__ == "__main__":
    pool = LoopThreadPool()
    actors = []
    loops = []
    for i in range(0, 100):
        loop = pool.create_loop()
        loops.append(loop)
        kwargs = {'loop' : loop}
        actor = TestActor(*[], **kwargs)
        actors.append(actor)
        actor.start()
    
    ind = 0
    while True:
        i = ind % len(actors)
        actor = actors[i]
        loop = loops[i]
        ind += 1
        do_tell(actor, loop)
