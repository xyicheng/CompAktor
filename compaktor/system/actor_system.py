'''
The actor system stores the groups of actors.  It is the root of the hierarchy.

Created on Aug 20, 2017

@author: aevans
'''


import asyncio
import gc
from threading import Lock
from compaktor.errors.actor_errors import ChildNodeExistsException,\
    ChildNotFoundException
from compaktor.state.actor_state import ActorState


class ActorTreeNode(object):
    """
    A B+ tree storing actor nodes.
    """
    def __init__(self, name, actor, children  = {}):
        self.children = children
        self.actor = actor
        self.name = name

    def __str__(self, *args, **kwargs):
        return "ActorTreeNode(name = {}, actor = {}, children = {})".format(self.name, self.actor.__str__(), self.children.__str__()) if self.actor is not None else "ActorTreeNode(name = {}, actor = {}, children = {})".format(self.name, "", self.children.__str__())

    def __repr__(self, *args, **kwargs):
        return self.__str__(*args, **kwargs)


class ActorSystem:
    """
    A basic actor system tree.  Actors are referenced as if they belong to a 
    file system.
    """

    __root = None
    __system_name = None

    def __init__(self, system_name):
        """
        Constructor.

        :param system_name:  The system name
        """
        self.__root = ActorTreeNode(system_name.strip(),None)
        self.__system_name = system_name

    def create_branch(self, root_node):
        """
        Create a branch in the actor system.  This is basically an additional 
        system.  The name of this system is take from the tree node provided.

        :param root_node:  The root node for the new branch.
        :type root_node:  ActorTreeNode
        """
        if root_node.name in self.__root.children:
            raise ChildNodeExistsException("Branch Exists for Actor \
            {}.".format(root_node.name))

        self.__root.children[root_node.name.trim()] = root_node
    
    def add_actor(self, actor, path):
        """
        Add an actor at the path in the system.  A node name may be optionally
        provided.  The name is pulled from the actor.

        :param actor:  The actor 
        :type actor:  Actor
        :param path:  Path separated by /
        :type path:  str
        """
        if path is None:
            raise TypeError("Path was None but must be a string providing a\
             path in the tree")

        path_list = path.split('/')
        current_node = self.__root
        for i in range(1,len(path_list)):
            node_name = path_list[i].strip()
            if node_name not in current_node.children:
                raise ChildNotFoundException("Child Node not found for {}".format(node_name))
            current_node = current_node.children[node_name]

        if current_node.name != path_list[len(path_list) - 1]:
            raise ChildNotFoundException("Current Node {} is Not Intended Node\
             {}".format(current_node.name, path_list[len(path_list) - 1]))

        key_name = actor.get_name().strip()

        if current_node.name == key_name:
            raise Exception("New Actor Node {} Cannot have the same name as the\
             parent!".format(key_name))

        an =  ActorTreeNode(key_name,actor,{})
        current_node.children[key_name] = an  

    def get_actor_node(self, path):
        """
        Get an actor at the specified path.

        :param path:  The path to the actor
        :type path:  str
        """
        if path is None:
            raise TypeError("Path is None but must be a path to the actor.")

        path_list = path.split('/')
        if len(path_list) is 1 and len(path_list[0]) is 0:
            raise ChildNotFoundException("Path List is Empty.")
        elif len(path_list) is 1:
            #Pep8 80 char from left margin sucks
            raise ChildNotFoundException(
                                        "Path cannot be root but must contain\
                                        root {}.".format(self.__root.name)
                                        )
        current_node = self.__root
        for i in range(1, len(path_list)):
            node_name = path_list[i].strip() 
            if node_name not in current_node.children:
                raise ChildNotFoundException("Child does not exist at \
                {}".format(node_name))
            current_node = current_node.children[node_name]

        if current_node.name != path_list[len(path_list) - 1]:
            raise ChildNotFoundException("Intended node name {} does not match \
            actual node name {}".format(path_list[len(path_list) - 1], 
                                        current_node.name))

        return current_node

    def stop_actor(self, path):
        """
        Stop an actor at a specified path.  Verifies the path to the actor

        :param path:  Path to the actor
        :type path:  str  
        """
        if path is None:
            raise TypeError("""Path to Actor cannot be None.  It must be a 
            string path separated by /.""")
        
        actor = self.get_actor_node(path)
        if actor is not None:
            asyncio.get_event_loop().run_until_complete(actor.actor.stop())

    def stop_actors_on_branch(self, branch_name):
        """
        Stop all actors on a specific branch.

        :param branch_name:  The name of the branch / system
        :type branch_name:  str
        """
        current_node = self.__root
        branch = branch_name.strip()
        if branch not in current_node.children:
            raise ChildNotFoundException("Branch not Found when Stopping Actors\
            for {}".format(branch_name))

        def recurse(node):
            if node is not None and node.actor is not None:
                if node.actor.get_state() is ActorState.RUNNING:
                    asyncio.get_event_loop().run_until_complete(node.actor.stop())
                    
                for child_node in node.children:
                    recurse(node.children[child_node])

        current_node = current_node.children[branch_name.strip()]
        recurse(current_node)

    def stop_all(self):
        """
        Stop all actors starting at a specific node.

        :param node:  The node to begin terminating actors on
        :type node:  The ActorTreeNode to start canceling from
        """
        for branch_name in self.__root.children:
            self.stop_actors_on_branch(branch_name)

    def delete_branch(self, branch_name):
        """
        Completely Removes a branch in the tree.

        :param branch_name:  The name of the branch
        """
        if branch_name in self.__root.children:
            self.stop_actors_on_branch(branch_name)
            del self.__root.children[branch_name]
            gc.collect()

    def add_branch(self, branch_name, actor = None):
        """
        Add a branch to the tree.  The root may or may not contain an actor.  
        It is recommended to use the actor name as the branch name.

        :param branch_name:  The name of the branch to use
        :type branch_name:  str
        :param actor:  The actor to add at the branch root
        :type actor:  Actor
        """
        self.__root.children[branch_name] = actor 

    def print_tree(self):
        """
        Prints the current actor tree.
        """    
        current_node = self.__root
        print(current_node)
        def print_node(node, indent_level):
            if node is None or node.actor is None:
                return

            n = node.__str__()
            n_indent = len(n) + indent_level
            print(n.rjust(n_indent))

            key_list = list(node.children.keys())
            for i in range(len(key_list)):
                child = key_list[i]
                print_node(node.children[child], indent_level + 4)

        for child in current_node.children:
            print_node(current_node.children[child], 4)

    def print_active_nodes(self):
        """
        Recurse the tree and print only active nodes.
        """
        current_node = self.__root

        def print_node(node, ident_level):

            if node is None or node.actor is None:
                return

            if node.actor.get_state() is ActorState.RUNNING:
                print(node.name.rjust(ident_level))

            for child in node.children():
                print_node(node.children[child], ident_level + 4)

        print_node(current_node, 0)

    def get_children(self, branch_path):
        """
        Get the children of a branch.  This can help in the creation of 
        routers.  An empty list is always returned.

        :param branch_path:  The path to the nodes in the system
        :type branch_path:  str
        :return:  A list either empty or not.
        """
        node = self.get_actor_node(branch_path)
        if node is not None:
            return node.children
        else:
            return []

    def close(self, do_print = False):
        """
        Iterate down the tree, close all actors, and remove the actors from the
        tree.

        :param do_print:  Whether to print the tree as it closes
        :type do_print:  boolean
        """
        def recurse(node):

            if do_print is True:
                print(node.name)

            if node is not None:
                if node.actor is not None:
                    if do_print is True:
                        print("Closing {}".format(node.actor.get_name()))

                    if node.actor.get_state() is ActorState.RUNNING:
                        asyncio.get_event_loop().run_until_complete(node.actor.stop())

                for child_node in node.children:
                    recurse(node.children[child_node])
        recurse(self.__root)
