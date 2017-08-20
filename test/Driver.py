'''
Created on Aug 19, 2017

@author: aevans
'''


import asyncio
import unittest
from compaktor.actor.actor import BaseActor
from compaktor.actor.message import Message


class ActorTest(unittest.TestCase):
    
    def create_actor_test(self):
        # Define new message classes, and register handlers for them in your actors.
        class StringMessage(Message): pass

        # Define actors that respond to Message subclasses with custom behavior.
        class PrintActor(BaseActor):
    
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.register_handler(StringMessage,
                                      self._string_message_handler)
        
        
            
            def _string_message_handler(self, message):
                print(message.payload)
                
            
        async def say_hello():
            a = BaseActor()
            b = PrintActor()
            a.start()
            b.start()
            for _ in range(10):
                message = StringMessage('Hello world!')
                await asyncio.sleep(0.25)
                await a.tell(b, message)
            await a.stop()
            await b.stop()

        asyncio.get_event_loop().run_until_complete(say_hello())
    

if __name__ == "__main__":
     unittest.main()