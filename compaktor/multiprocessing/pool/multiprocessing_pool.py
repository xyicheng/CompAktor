'''
Multiprocessing pool.  Capabe of managing processes for the entire project.
Created on Oct 12, 2017

@author: aevans
'''

import logging
from multiprocessing import cpu_count, Lock, Pool, Process
import traceback


__POOL__ = None
__pool_lock = Lock()

def submit_process(func, args=None, max_processes=cpu_count()):
    """
    Submit a process and potentially create a pool.  The pool can only be
    instantiated once.

    :param func: The function to execute
    :type func: method or function
    :param args: The arguments for the func
    :type args: list()
    :param max_processes: Maximum process count
    :type max_processes: int()
    """
    proc = None
    __pool_lock.acquire()
    try:
        global __POOL__
        if __POOL__ is None:
            logging.info("Instantiating Pool")
            __POOL__ = Pool(processes=max_processes)
        proc = __POOL__.apply_async(func, args)
    except Exception as e:
        logging.error(e)
        traceback.print_exc()
    __pool_lock.release()
    return proc
