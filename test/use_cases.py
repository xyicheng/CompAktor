'''
Created on Aug 26, 2017

@author: aevans
'''

import asyncio
import logging
import multiprocessing
import sys
from threading import Lock
from compaktor.actor.actor import HandlerNotFoundError
from compaktor.actor.message import Message, QueryMessage, PoisonPill
import nltk
from compaktor.actor.actor import BaseActor
from jinja2.nodes import Block
from asyncio.coroutines import _AwaitableABC


class SubscribeMessage(Message):
    subscriber = None


class BlockMessage(Message): pass


class UnblockMessage(Message): pass


class PullMessage(Message): pass


class CompleteMessage(Message): pass


class Flow(object):
    
    
    def __init__(self, source):
        self.source = source
        self.current_stage = self.source
    

    def via(self, stage):
        #create the pubsub for our connector if not exists
        #or insert into pub sub
        if self.current_stage.output_stage is None:
            actor = PubSub()
            actor.start()
            actor.set_publisher(self.current_stage)
            self.current_stage.output_stage = actor 
            self.current_stage.output_stage.add_subscriber(stage)
        else:
            self.current_stage.output_stage.add_subscriber(stage)
        
    
    def to(self, sink):
        #allow multiple sinks in our pub/sub model
        if self.current_stage.output_stage is None:
            actor = PubSub()
            actor.start()
            actor.set_publisher(self.current_stage)
            self.current_stage.output_stage = actor
            self.current_stage.output_stage.add_subscriber(sink)
        else:
            self.current_stage.output_stage.add_subscriber(sink)
    
    
    async def run(self):
        
        if self.source is None:
            raise TypeError("Source Cannot Be Null")
        
        if self.source.output_stage is None:
            raise TypeError("Source Output Stage or Sink Cannot be Null")
        
        self.source.start()
    
    
    async def terminate(self):
        await self.source.tell(self.source, PoisonPill())


class PubSub(BaseActor):
    """
    A broadcasting publisher. 
    """
    
    
    def __init__(self, *args, **kwargs):
        super().__init__(self, *args, **kwargs)
        self.register_handler(SubscribeMessage, self.add_subscriber)
        self.register_handler(PoisonPill, self.close)
        self._subscribers = []
        self._publisher = None
        
    
    def set_publisher(self, publisher):
        self._publisher = publisher
    
    
    def add_subscriber(self, message):
        self._subscribers.append(message.subscriber)
    
    
    async def close(self, message):
        #broadcast our Poison pill across the net
        for actor in self._subscribers:
            await self.tell(actor, PoisonPill())
        
        await self.stop()
    
    
    async def publish(self,message):
        for actor in self._subscribers:
            await actor.tell(actor, message)
    

class Stage(BaseActor):
    """
    Stage for controlling flow between pubsubs.
    """
    
    
    def __init__(self, *args, **kwargs):
        super().__init__(self, *args, **kwargs)
        self.register_handler(PullMessage, self.pull)
        self.register_handler(CompleteMessage, self.complete)
        self.register_handler(PoisonPill, self.complete)
        self._in = None
        self._out = None
        
    
    def set_output_stage(self, stage):
        self._out = stage


    def set_previous_stage(self, stage):
        self._in = stage    
    
    
    def force_complete(self):
        """
        Force complete on this part of the stream
        """
        #send compelte forward
        asyncio.get_event_loop().run_until_complete(self.complete(PoisonPill()))
    
    
    async def on_downstream_finish(self):
        """
        Behavior to use on downstream finish
        """
        self.stop()
    
    
    async def complete(self, message):
        await self.tell(self.output_stage, PoisonPill)
        await self.stop()
    
    
    async def pull(self,message):
        output = self.handle_pull(message)    
        
        if self.output_stage is not None:
            await self.tell(self.sink, output)
        elif self.sink is not None:
            await self.tell(self.sink, output)


class Source(BaseActor):
    
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.register_handler(PullMessage, self.pull)
        self.register_handler(PoisonPill, self.complete)
        self._out = None
        
    
    async def complete(self):
        await self.stop()
        
    
    def set_output_stage(self, stage):
        """
        Set the output router that the system connects to.
        """
        self._out = stage
    
    
    def handle_pull(self, message):
        """
        Handle the message and return the result
        """
        return message
    
    
    def pull(self, message):
        """
        Specify pulling behavior. return the proper output
        """
        output = self.handle_pull(message)    
        
        if self.output_stage is not None:
            self.tell(self.output_stage, output)
        elif self.sink is not None:
            self.tell(self.sink, output)


class Sink(BaseActor):
    """
    Sinks work with the Pub/Sub Model.  When ready. The subscription queue is waited on.
    Backpressuring a sink requires 
    """
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)  
        self.register_handler(PullMessage, self.pull)
        self._in = None
        
    
    def set_previous_stage(self, stage):
        self._in = stage

    
    def pull(self, message):
        """
        Specify behavior for how to handle the pull
        """
        pass


class PrintSink(Sink):
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
    
    
    def pull(self, message):
        print(message)


if __name__ == "__main__":
    source = Source()
    flow = Flow(source)
    flow.to(PrintSink())
