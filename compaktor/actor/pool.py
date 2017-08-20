"""
This is the actor and host pool registry.  This pool stores registered 
actors and accesses the as needed.  Actors register with a pool when 
requests are made to the seed machines.  These machines store the different 
hosts. 

Created on Aug 18, 2017

@author: aevans
"""


class RegistryPool:
    """
    This class stores regstry information.  Only one registry is instantiated 
    on a system at a time.
    """
    
    
    __SEED_NODES = []
    
    def __init__(self):
        pass
    
    
    def add_seed_node(self):
        pass
    
    
    def get_seed_nodes(self):
        pass

    
    def discover_nodes(self):
        pass
    

    def remove_node(self):
        pass


class ActorPool:
    """
    The actor pool stores actors for this node.  Each process has a set of 
    actors already running.  When a message is received, the pool is used
    to send the work to an actor. 
    """

    
    def __init__(self):
        pass
    
    
    def create_actor(self):
        pass
    
    
    def remove_actor(self):
        pass
    
    
    def add_to_pool(self):
        pass
