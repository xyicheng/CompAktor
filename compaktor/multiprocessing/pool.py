'''
A common multi-processing pool shared by the actors. This is where
individual tasks have acess to all cores.

Created on Sep 16, 2017

@author: aevans
'''


from concurrent.futures import ProcessPoolExecutor
import multiprocessing
from concurrent.futures._base import Future


MAX_PROCESSES = multiprocessing.cpu_count()

class MultiProcessPool(object):
    """
    A pool shared by the program. One instance is allowed per program.
    This pool could be overridden as needed.
    """
    
    __executor = None
    
    @staticmethod
    def setup(max_processes, executor = None):
        """
        Constructor

        :param max_processes: The maximum number of processes in the pool
        :type max_processes: int()
        :param executor: Process executor
        :type executor: ProcessPoolExecutor()
        """
        if executor:
            MultiProcessPool.__executor = executor
        else:
            MultiProcessPool.__executor = ProcessPoolExecutor(max_processes)

    @staticmethod
    def submit_task(func, args):
        """
        Run the task in a given process. Return a future

        :param func: A func to execute
        :type func: The function to execute
        :param args: The function arguments
        :type args: list()
        """
        if MultiProcessPool.__executor is None:
            MultiProcessPool.setup(MultiProcessPool.MAX_PROCESSES)
        return MultiProcessPool.__executor.submit(func, args)

    @staticmethod    
    def wait_and_terminate(tasks, task_timeout=15, pool_timeout=15):
        """
        Waits for remaining tasks to complete and then shuts down.

        :param tasks: A list of the futures
        :type tasks: list()
        :param task_timeout: The timeout for each task
        :type task_timeout: int()
        :param pool_timeout: The time to wait for the pool to shutdown
        :type pool_timeout: int()
        :return: The list of futures with their results
        :rtype: list()
        """

        ret_futs = [fut.result(task_timeout) for fut in tasks]
        MultiProcessPool.__executor.shutdown(pool_timeout)
        return ret_futs
