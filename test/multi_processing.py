'''
Created on Sep 16, 2017

@author: aevans
'''

import unittest
from threading import Thread
from compaktor.multiprocessing.pool import multiprocessing_pool as pool
from queue import Queue
import pickle
from scrapy.utils import testproc


class TestProcessing():

    def print_func(self, msg):
        r_str = "Received {}".format(msg[0])
        return r_str
    
    
    def load_func(self, it):
        return it
    
    
    def test_process_in_pool(self):
        print("Testing Processes in Pool")
        args = ["Hello World"]
        fut = pool.submit_process(self.print_func, args)
        rval = fut.result()
        assert rval == "Received Hello World"
    
    
    def test_load(self):
        futs = []
        total = 0
        for i in range(0,10):
            total += i        
            futs.append(pool.submit_process(self.load_func, [i]))
        results = [proc.result(timeout=1)[0] for proc in futs]
        for r in results:
            assert isinstance(r, int), "Future Result should be an int"
        res = sum(results)
        assert res == total, "Result {} should be {}".format(res, total) 
    
    
    def test_threaded(self):
        test_q = Queue()
        def run_proc(it):
            proc = pool.submit_process(self.load_func, [it])
            test_q.put_nowait(proc.result())
    
        threads = []
        for i in range(0, 3):
            t = Thread(target=run_proc, args=[i])
            t.start()
            threads.append(t)
        [t.join() for t in threads]
        res = sum([test_q.get_nowait()[0] for t in threads])
        assert(res == 3)

if __name__ == "__main__":
    print("Testing")
    tp = TestProcessing()
    tp.test_process_in_pool()
    tp.test_threaded()
    tp.test_load()
    print("Complete")
