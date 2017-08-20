'''
Actor based utilities to ensure that actors are running.  These utilities
perform tasks like getting a new name for an actor.

Created on Aug 19, 2017

@author: aevans
'''


CURRENT_ACTOR = 0


def get_actor_name(base_name):
    CURRENT_ACTOR += 1
    return "{}_{}".format(base_name, CURRENT_ACTOR)
