'''
An event loop based health checker which runs on its own loop.  This will
update a status as actors are discovered to be non-functional.

Created on Aug 21, 2017

@author: aevans
'''

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
        self.visited = {}
        self.per_actor = per_actor
        self.actors_per_check = self.actors_per_check
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
        if system_path in self.actor_system.children:
            pass
    
    
    async def check_running_actors(self,checked_actors):
        """
        In this case, the entire tree is searched.  The visited and
        current_actor variables are used here.
        """
        actor = self.current_check_actor
        if actor.get_state() is ActorState.TERMINATED:
            self._terminated.append(actor)
        elif actor.get_state() is ActorState.STOPPED:
            self._stopped.append(actor)
        
        for child in actor.children:
            pass


    async def run(self):
        """
        Run the heartbeat checker.   
        """
        if self.per_actor is False:
            system = self.actor_system.children[self.index]
            self.check_running_tree(system)
            self.index += 1
        else:
            self.check_running_actors(0)
