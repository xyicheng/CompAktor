'''
A set of standard messages

Created on Aug 19, 2017

@author: aevans
'''

class Message(object):
    
    def __init__(self, payload = None):
        self.payload = payload

    
    def __repr__(self):
        return "Message ({})".format(self.payload) if self.payload is not None else "Message ()"
    
    
class QueryMessage(Message): 
    result = None


class Broadcast(Message): pass


class PoisonPill(Message): pass
