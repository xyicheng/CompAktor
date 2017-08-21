'''
The actor system stores the groups of actors.  It is the root of the hierarchy.

Created on Aug 20, 2017

@author: aevans
'''


class ChildNotFoundException(Exception): pass


class ChildNodeExistsException(Exception): pass


class ActorTreeNode():
    """
    A B+ tree storing actor nodes.
    """
    actor = None
    name = None
    
    def __init__(self, name, actor):
        self.children = {}
        self.actor = None
        self.name = None


class ActorSystems():
    """
    The total systems storing the actor nodes.  Addresses correlate to tree 
    structures starting at the root of the hierarchy.
    """
    
    
    __NAME = None
    __root = ActorTreeNode("root", None)
    
    
    def __init__(self, name):
        self.__NAME = name
        
    
    def add_actor(self, actor, name, path = None):
        """
        Add an actor to the tree at the given path (address).
        
        :param actor:  The actor to add to the tree
        :param name:  The name of the actor which becomes the final path part
        :param path:  The path to search for defaulting to root only
        """
        current_node = self.__root
        if path is not None:
            #add to a specific path if it exists
            path_parts = path.split('/')
            current_node = self.__root
            for part in path_parts:
                if part in current_node.children:
                    current_node = current_node.children[part]
                else:
                    raise ChildNotFoundException("Child not found for path {} in\
                                                  {}!".format(part,path))    
        
        if name in current_node.children:
            raise ChildNodeExistsException("Child node already exists.  Cannot\
                                             add to path!")
        else:
            #add actor to path
            current_node.children[name] = actor    
    
    
    def stop_all_actors(self, actor):
        """
        Stop all actors in a given subtree starting with the root actor.
        
        :param actor:  The actor subtree root
        :type actor:  ActorTreeNode
        """
        actor.actor.stop()
        if actor.children is not None:
            for actor in actor.children:
                self.stop_all_actors(actor)
    
    
    def remove_actors(self, path):
        """
        Remove actors at a path.
        
        :param path:  The path to the subtree to remove
        :type path: str
        """
        if path is None:
            raise Exception("Path to remove is None but the actor is required.")
        
        path_parts = path.split('/')
        current_node = self.__root
        parent = current_node
        for i in range(0, len(path_parts) - 1):
            part = path_parts[i]
            if part not in current_node.children:
                raise ChildNotFoundException("Child node not in {}!".format(path))
            parent = current_node
            current_node = current_node.children[part]
        
        actor_name = path_parts[len(path_parts) - 1]
        self.stop_all_actors(current_node)
        del parent[actor_name]


    def get_actors_tree(self, path):
        """
        Return the subtree at the actor path.
        
        :param path:  Path to the root of the actors to return
        :return:  The node subtree at the path
        """
        if path is None:
            raise Exception("Path cannot be null in getting actor subtree.")
        
        path_parts = path.split('/')
        current_node = self.__root
        for path in path_parts:
            if path not in current_node.children:
                raise ChildNotFoundException("Child not in path {}!".format(path))
            current_node = current_node.children[path]
        return current_node
    
    
    def get_actors(self,path):
        """
        Get only the child actors at a level.
        
        :param path:  The path to the current actor
        :return:  The actors 
        """
        current_node = self.get_actors_tree(path)
        if len(current_node.children) > 0:
            for node in current_node.children:
                yield node.actor
        else:
            yield None
