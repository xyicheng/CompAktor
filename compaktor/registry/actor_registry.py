'''
The actor registry stores nodes in a b+ tree for
retrieval and usage.

Created on Oct 14, 2017

@author: aevans
'''

import asyncio
import os
import socket
from compaktor.registry.objects.node import RegistryNode as Node
from compaktor.state.actor_state import ActorState

__REGISTRY = None

class Registry:
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
        children = node.children
        addr_part = addr_arr[idx]
        if len(children) is 0:
            return None
        for child in children:
            if child.name == addr_part:
                idx += 1
                return self.__find_node(addr_arr, node, idx)
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
            try:
                addr_arr = list(address)
            except:
                raise TypeError("Address for registry must be of type str")
        self.find_node(addr_arr)

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
        if actor.name is None:
            raise ValueError("Actor must have a name")

        if isinstance(address, str):
            address = address.split(self.__sep)
        if len(address) > 1:
            node = self.find_node(address)
            if node is None:
                raise ValueError(
                    "Node not Found for Path [{}]".format(str(address)))
            new_node = Node(actor.name, actor, is_local)
            address.append(actor.name)
            actor.address = address
            node.paranet.append(new_node)
        elif len(address) == 1:
            raise ValueError(
                "Address must contain full Path [{}]".format(str(address)))
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
                asyncio.run_coroutine_threadsafe(node.actor.stop())
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

def get_registry():
    if __REGISTRY is None:
        __REGISTRY = Registry()
    return __REGISTRY
