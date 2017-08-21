'''
Sadly, unit test is not working.  The first test is just ported over from
Cleveland.  Other tests include router tests, remote actor tests, and more.
Some tests may be chained but others cannot be.

Created on Aug 19, 2017

@author: aevans
'''


import asyncio
from multiprocessing import Process
import socket
from compaktor.actor.actor import BaseActor
from compaktor.actor.message import Message


#Define new message classes, and register handlers for them in your actors.
class StringMessage(Message): pass

def write_to_connection(host = 'localhost', port = 9090, loop = None):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect(('',9090))
        sock.send(bytearray("Hello World!","utf-8"))
    
    
def create_actor_test():
    
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
    pass