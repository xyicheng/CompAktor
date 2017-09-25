'''
Created on Sep 21, 2017

@author: aevans
'''


from enum import Enum


class ActorState(Enum):
    """
    The state of the actors.  The following states exist.

    - STOPPED:  The actor is stopped after running but post_stop was not called
    - RUNNING:  The actor is running
    - LIMBO:  The actor is starting but not yet started
    - TERMINATED:  The actor has been terminated and post_stop called
    - CREATED:  The actor is created but not started
    """
    STOPPED = 0
    RUNNING = 1
    LIMBO = 2
    TERMINATED = 3
    CREATED = 4
