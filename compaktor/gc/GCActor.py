'''
Created on Sep 2, 2017

@author: aevans
'''
import gc


from compaktor.actor.actor import BaseActor
from compaktor.actor.message import Message


class GCRequest(Message):
    pass


class GCActor(BaseActor):
    """
    The garbage collection actor.  This actor handles GCRequest messages.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.register_handler(GCRequest, self.handle_gc)

    def handle_gc(self, message):
        gc.collect()
        del gc.garbage[:]
