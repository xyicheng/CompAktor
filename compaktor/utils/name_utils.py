'''
Name Creation Utils

Created on Sep 21, 2017

@author: aevans
'''


from atomos.atomic import AtomicLong
from random import random

# long will be fine for now
class NameCreationUtils():
    """
    Utilities for getting the base name of the class
    """
    NAME_BASE = AtomicLong()

    @staticmethod
    def get_name_base():
        base = NameCreationUtils.NAME_BASE.get_and_add(1)
        if NameCreationUtils.NAME_BASE.get() is float('inf'):
            NameCreationUtils.NAME_BASE.set(0)
        return base

    @staticmethod
    def get_name_and_number(name):
        if name:
            name += "_"
            name += str(int(random() * 1000))
        else:
            raise ValueError("Name Cannot be Null")
        return name
