'''
The actor system stores the groups of actors.  It is the root of the hierarchy.

Created on Aug 20, 2017

@author: aevans
'''


import gc
from threading import Lock


class ChildNotFoundException(Exception): pass


class ChildNodeExistsException(Exception): pass


class ActorTreeNode:
    """
    A B+ tree storing actor nodes.
    """
    actor = None
    name = None
    
    
    def __init__(self, name, actor):
        self.children = {}
        self.actor = actor
        self.name = name


class ActorSystem:
    """
    A basic actor system tree.  Actors are referenced as if they belong to a file system.
    """
    
    __root = None
    __system_name = None
        
        
    def __init__(self, system_name):
        """
        Constructor.
        
        :param system_name:  The system name
        """
        self.__root = ActorTreeNode(system_name,None)
        self.__system_name = system_name
    
    
    def create_branch(self, root_node):
        """
        Create a branch in the actor system.  This is basically an additional 
        system.  The name of this system is take from the tree node provided.
        
        :param root_node:  The root node for the new branch.
        :type root_node:  ActorTreeNode
        """
        if root_node.name in self.__root.children:
            raise ChildNodeExistsException("Branch Exists for Actor {}.".format(root_node.name))
        
        
        self.__root.children[root_node.name.trim()] = root_node
        
    
    def add_actor(self, actor, path, node_name = None):
        """
        Add an actor at the path in the system.  A node name may be optionally
        provided.
        
        :param actor:  The actor 
        :type actor:  Actor
        :param path:  Path separated by /
        :type path:  str
        :param node_name:  The name of the node to add
        :type node_name:  str
        """
        if path is None:
            raise TypeError("Path was None but must be a string providing a path in the tree")
        
        path_list = path.split('/')
        current_node = self.__root
        for path_part in path_list:
            node_name = path_part.trim()
            if node_name not in current_node.children:
                raise ChildNotFoundException("Child Node not found for {}".format(node_name))
            current_node = current_node.children[node_name]
    
    
    def get_actor(self, path):
        """
        Get an actor at the specified path.
        
        :param path:  The path to the actor
        :type path:  str
        """
        if path is None:
            raise TypeError("Path is None but must be a path to the actor.")
        
        path_list = path.split('/')
        for path in path_list:
            node_name = path.strip() 
        
    
    def stop_actor(self, path):
        """
        Stop an actor at a specified path.  Verifies the path to the actor
        
        :param path:  Path to the actor
        :type path:  str  
        """
        if path is None:
            raise TypeError("""Path to Actor cannot be None.  It must be a 
            string path separated by /.""")
        
        actor = self.get_actor(path)
    
    
    def stop_actors_on_branch(self, branch_name):
        """
        Stop all actors on a specific branch.
        
        :param branch_name:  The name of the branch / system
        :type branch_name:  str
        """
        pass

    
    def stop_all_actors(self):
        pass
    
    
    def remove_all_actors(self):
        pass
    
    
    def delete_branch(self):
        pass
    
    
    def add_branch(self):
        pass
