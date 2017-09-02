'''
Created on Aug 30, 2017

@author: aevans
'''
from compaktor.actor.actor import BaseActor
from compaktor.actor.message import Message


class Publish(Message): pass


class Demand(Message): pass


class Subscribe(Message): pass


class PubSub(BaseActor):
    """
    Special Publisher/Subscriber for streaming
    """
    
    
    def __init__(self, *args, **kwargs):
        """
        The constructor
        
        :Keyword Arguments:
            *publisher (BaseActor): The publisher submitting messages to the pub/sub
        """
        super().__init(*args, **kwargs)
        self._subscribers = []
        self._publisher = None
        self.register_handler(Demand, self.handle_demand)
        self.register_handler(Subscribe, self.subscribe)
        self.register_handler(Publish, self.publish)
        
    
    def subscribe(self, actor):
        """
        Subscribe to the pub/sub.  This will replace the subscriber queue to create 
        a balancing router. 
        """
        if actor not in self.subscribers:
            self._subscribers.append(actor)
        
    
    def publish(self, message):
        """
        Submit a message to the common subscription queue
        """
        pass
    
    
    def handle_demand(self, message):
        """
        Handle demand by 
        """
        pass

    
    def handle_broadcast(self, message):
        """
        Broadcast to all actors.  Do not connect the actors subscribing here to a broadcast router.
        """
        for actor in self._subscribers:
            self.tell(actor, message)
