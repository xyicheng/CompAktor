'''
Created on Sep 16, 2017

@author: aevans
'''

import pytest
from compaktor.multiprocessing.pool import MultiProcessPool, MAX_PROCESSES


@pytest.fixture
def pool():
    MultiProcessPool.MAX_PROCESSES = 10
    return MultiProcessPool


def print_func(msg):
    return "Received {}".format(msg[0])


def load_func(it):
    return it


def test_process_in_pool(pool):
    fut = pool.submit_task(print_func, ["Hello World"])
    rval = fut.result()
    assert rval == "Received Hello World"


def test_load(pool):
    futs = []
    total = 0
    for i in range(0,1000):
        total += i
        futs.append(pool.submit_task(load_func, i))
    results = pool.wait_and_terminate(futs)
    for r in results:
        assert isinstance(r, int), "Future Result should be an int"
    res = sum(results)
    assert res == total, "Result {} should be {}".format(res, total) 
