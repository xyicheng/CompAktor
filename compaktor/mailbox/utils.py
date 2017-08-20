'''
Common mailbox utilities for connecting and sending messages.

Created on Aug 19, 2017

@author: aevans
'''


import rabbitmq
import pika


CONNECTION = None
CHANNEL = None


def setup(host):
    """
    Setup the connection to the python queue.  This method sets up the
    the connection and the channel.
    
    :param host:  The host to connect to.
    :type host:  str
    :raises:  IOError 
    """
    if CONNECTION is None:
        CONNECTION = pika.BlockingConnection(pika.ConnectionParameters(host = host))
        CHANNEL = CONNECTION.channel()
    else:
        raise IOError("CONNECTION already established and must be closed first.")


def declare_queue(name):
    """
    Instantiate a queue with a given name.
    
    :param name:  The queue name
    :type name:  str
    :raises:  IOError
    """
    if CHANNEL is not None:
        CHANNEL.queue.queue_declare(name)
    else:
        raise IOError("CHANNEL not yet established.  Call setup().")    


def publish(exchange, routing_key, message):
    """
    Publish a message to an exchange.  This may be to any supported structure.
    
    :param exchange:  The exchange name
    :type name:  str
    :param routing_key:  The specific routing key
    :type routing_key: str
    :param message:  The message to publish
    :type message: str
    :raises:  IOError
    """
    if CHANNEL is not None:
        CHANNEL.basic_publish(exchange, routing_key, message)
    else:
        raise IOError("CHANNEL not yet established.  Call setup().")    


def set_callback(callback, exchange, no_ack = True):
    """
    Set the callback for an exchange.  A callback has the following parameters
    ch, method, properties, and body.
    
    :param callback:  The callback 
    :param exchange:  The exchange name
    :type exchange:  str
    :param no_ack:  Whether to acknowledge the callback
    :type no_ack : boolean
    :raises:  IOError
    :doc:  https://www.rabbitmq.com/tutorials/tutorial-one-python.html
    """
    if CHANNEL is not None:
        CHANNEL.basic_consume(callback, exchange, no_ack)
    else:
        raise IOError("CHANNEL not yet established. Call setup().")
        

def close_connection():
    """
    Close the RabbitMQ connection if opened.
    
    :raises: IOError
    """
    if CONNECTION is not None:
        CONNECTION.close()
        CHANNEL = None
    else:
        raise IOError("CONNECTION not yet established.  Nothing to close.")     
