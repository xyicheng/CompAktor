'''
The actor registry stores nodes in a b+ tree for
retrieval and usage.

Created on Oct 14, 2017

@author: aevans
'''

import os
from compaktor.registry.objects.node import RegistryNode as Node
from compaktor.state.actor_state import ActorState
from compaktor import registry
import pdb


__REGISTRY = None


class Registry(object):
    """
    The registry to use.
    """
    def __init__(self, host):
        """
        Constructor

        :param host: The host for the registry
        :type host: str()
        """
        self.__root = Node(host, None, True)
        self.__sep = os.path.sep
        self.__host = host

    def __enter__(self,host):
        """
        Enter for use alongside with

        :param host: The host to use
        :type host: str()
        """
        self.__init__(host)

    def get_host(self):
        return self.__host

    def get_sep(self):
        return self.__sep

    def set_sep(self, sep):
        """
        Set the separator.

        :param sep: The separator to use
        :type sep: str()
        """
        if isinstance(sep, str):
            self.__sep = sep
        else:
            raise TypeError("Separator Must be String")        

    def move_branch(self, old_address, new_parent_address):
        """
        Move an actor in the registry froman old address to be underneath a new parent.

        :param old_address: Currently existing address in the registry
        :type old_address: str or list
        :param new_parent_address: Address of the new parent node in the registry
        :type new_parent_address: str or list
        """
        old = old_address
        if isinstance(old, str):
            old = old.split(self.get_sep())
        else:
            old = list(old)

        new_p = new_parent_address
        if isinstance(new_p, str):
            new_p = new_p.split(self.get_sep())
        else:
            new_p = list(new_p)
        old_n = self.find_node(old)
        new_n = self.find_node(new_p)
        self.remove_branch(old, stop=False)
        self.__rename_nodes(new_n.__address, old_n)
        self.add_actor(new_n.address,old_n, new_n.is_local)

    def __rename_nodes(self, new_base, node):
        """
        Moving an actor branch requires moving the child nodes to a new address

        :param new_base: The to use with the node
        :type new_base: list()
        :param node: The node to rename and re-insert
        :type node: BaseActor()
        """
        address = node.address
        if address:
            if isinstance(address, str):
                address = address.split(self.get_sep())
            else:
                address = list(address)
            n_address = new_base
            if n_address:
                n_address.extend(address[:-1])
                node.address = n_address
                if node.children:
                    c_base = node.address
                    for child in node.children:
                        self.__rename_nodes(c_base, child)

    def __find_node(self, addr_arr, node, idx):
        """
        Find an actor node in the system

        :param addr_arr: The address in the system split by the separator
        :type addr_arr: list()
        :param node: The current node for the registry
        :type node: AbstractActor()
        :param idx: The current index in the address array
        :type idx: int()
        """
        if idx >= len(addr_arr):
            return None
        elif idx == len(addr_arr) - 1:
            if node.name == addr_arr[len(addr_arr) - 1]:
                return node
            else:
                return None
        else:
            idx += 1
            children = node.children
            addr_part = addr_arr[idx]
            if len(children) is 0:
                return None
            for child in children:
                if child.name == addr_part:
                    return self.__find_node(addr_arr, child, idx)
        return None

    def find_node(self, address):
        """
        Find an address in the network.

        :param address: The address to use
        :type address: str()
        :param actor: The actor to add
        :type actor: AbstractActr()
        :param is_local: Whether the node is local
        :type is_local: bool()
        """
        if isinstance(address, str):
            addr_arr = address.split(self.__sep)
        else:
            addr_arr = list(address)
        return self.__find_node(addr_arr, self.__root, 0)

    def add_actor(self, address, actor, is_local):
        """
        Add an actor in the network

        :param address: The address to use
        :type address: str or list
        :param actor: The actor to add
        :type actor: AbstractActr()
        :param is_local: Whether the node is local
        :type is_local: bool()
        """
        if address is None:
            raise ValueError("Address must be provided.")
        if actor.name is None:
            raise ValueError("Actor must have a name")
        if isinstance(address, str):
            address = address.split(self.__sep)
        if len(address) > 0:
            if self.__host not in address and self.__host:
                addr_t = [self.__host]
                address = addr_t.extend([x for x in address])
            node = self.find_node(address)
            if node is None:
                raise ValueError(
                    "Node not Found for Path {}".format(str(address)))
            new_node = Node(actor.name, actor, is_local)
            address.append(actor.name)
            new_node.actor.address = address
            node.children.append(new_node)
            new_node.parent = node
        else:
            raise ValueError("Address Is Empty")

    def __stop_all(self, node):
        """
        Performs stop on nodes and calls recursively

        :param node: The node to stop and call for children
        :type node: RegistryNode()
        """
        if node.actor:
            if node.actor.get_state() == ActorState.RUNNING:
                node.actor.loop.run_until_complete(node.actor.stop())
        if node.children:
            for child in node.children:
                self.__stop_all(child)

    def stop_all(self, node=None):
        """
        Stop all nodes starting with the provided node or root

        :param node: The start node defaulting to root
        :type node: RegistryNode()
        """
        if node is None:
            node = self.__root
        self.__stop_all(node)

    def remove_branch(self, address, stop=True):
        """
        Remove a branch beneath a node and optionally stop it and all children.

        :param address: The address to the node
        :type address: str or list
        :param stop: Stop any child nodes
        :type stop: bool()
        """
        node = self.find_node(address)
        if node:
            if node.children and stop:
                self.stop_all(node)
            node.parent.children.remove(node)
        else:
            raise ValueError("Node not found for path {}".format(address))

    def close(self):
        """
        Stop the actors in the system and reset the root.
        """
        self.stop_all()
        self.__root = Node(self.__host, actor=None, is_local=True)

    def __exit__(self):
        """
        Closeable exit
        """
        self.close()


def get_registry(host="localhost"):
    """
    Get the registry.
    """
    global __REGISTRY
    if __REGISTRY is None:
        __REGISTRY = Registry(host)
    return __REGISTRY
