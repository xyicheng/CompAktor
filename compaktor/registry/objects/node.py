'''
Generic Nodes
Created on Oct 14, 2017

@author: aevans
'''

class RegistryNode:

    def __init__(self, address, actor, is_local):
        self.address = address
        self.actor = actor
        self.is_local = is_local
        self.children = []
        self.parent = None
