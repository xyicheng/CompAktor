'''
An event loop based health checker which runs on its own loop.  This will
update a status as actors are discovered to be non-functional.

Created on Aug 21, 2017

@author: aevans
'''


import asyncio
from compaktor.actor.actor import ActorState


class HeartbeatMonitor(object):
    """
    Currently just a single node health checker.  It iterates down an actor
    tree looking for nodes that may be in jeopardy or down.
    """
    
    
    def __init__(self, actor_system, interval = 30, per_actor = False, actors_per_check = 100):
        """
        Constructor
        
        :param actor_system:  The actor system tree
        :type actor_system:  ActorSystem
        :param interval:  Number of seconds between checks; should be increased
        :type interval:  int
        :param per_actor:  Whether to check by actor or by tree branch
        """
        self.actor_system = actor_system
        self.interval = interval
        self.index = 0
        self.current_check_actor = None
        self._terminated = []
        self._stopped = []
    
    
    def handle_not_running(self, terminated):
        """
        Iterates down the terminated list and removes the terminated actors.
        
        :param terminated:  The list of terminated actors
        """
        pass
                    
                    
    async def check_running_tree(self, system_path):
        """
        Iterate down the supplied actor tree for a given system. Systems should
        be checked sequentially to avoid computational slow down. This method is
        called at the interval supplied to the class. An alternative would
        be to check a handful of actrs at a time and save the index position.
        
        :param system_path:  The system to be checked
        :type system_path: str
        :return:  A list of stopped and terminated actors
        """
        terminated = []
        def check_tree(actor, current_path):
            if actor.get_state() is ActorState.TERMINATED:
                actor_path = "{}/{}".format(current_path,actor.get_name())
                terminated.append(actor_path)
            
            
        if system_path in self.actor_system.children:
            pass
        
        return terminated
            

    async def handle_run(self):
        """
        A helper async function for run.   
        """
        terminated = []
        if self.per_actor is False:
            system = self.actor_system.children[self.index]
            terminated = self.check_running_tree(system)
            self.index += 1
        return terminated
    
    async def run(self):
        """
        Run the health checker.
        """
        while True:
            task = asyncio.Task(self.handle_run())
            #runs on main event loop
            loop = asyncio.get_event_loop()
            terminated = await loop.call_later(self.interval, task)
            
            if len(terminated) > 0:
                for actor in terminated:
                    self.
