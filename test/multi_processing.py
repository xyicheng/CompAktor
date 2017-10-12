'''
Created on Sep 16, 2017

@author: aevans
'''

import pytest
from threading import Thread
from compaktor.multiprocessing.pool import multiprocessing_pool as pool
from queue import Queue

def print_func(msg):
    r_str = "Received {}".format(msg)
    return r_str


def load_func(it):
    return it


def test_process_in_pool():
    fut = pool.submit_process(print_func, ["Hello World"])
    rval = fut.get()
    assert rval == "Received Hello World"


def test_load():
    futs = []
    total = 0
    for i in range(0,10):
        total += i        
        futs.append(pool.submit_process(load_func, [i]))
    results = [proc.get(timeout=1) for proc in futs]
    for r in results:
        assert isinstance(r, int), "Future Result should be an int"
    res = sum(results)
    assert res == total, "Result {} should be {}".format(res, total) 


def test_threaded():
    test_q = Queue()
    def run_proc(it):
        proc = pool.submit_process(load_func, [it])
        test_q.put_nowait(proc.get())

    threads = []
    for i in range(0, 3):
        t = Thread(target=run_proc, args=[i])
        t.start()
        threads.append(t)
    [t.join() for t in threads]
    res = sum([test_q.get_nowait() for t in threads])
    assert(res == 3)
