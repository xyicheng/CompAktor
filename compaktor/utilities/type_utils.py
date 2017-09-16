'''
Utilities class for dealing with types

Created on Sep 16, 2017

@author: aevans
'''


def is_num(num):
    """
    Test whether an object is a number.  Only primitives
    float, complex, or int count here
    """
    if isinstance(num, float) or isinstance(num, int):
        return True
    elif isinstance(num, complex):
        return True
    else:
        return False
