'''
A set of standard messages

Created on Aug 19, 2017

@author: aevans
'''


class Message(object):
    """
    Base Message to be extended
    """
    def __init__(self, payload=None, sender=None):
        """
        Constructor

        :param payload:  Message to send
        :type message: object
        :param sender:  The sender
        :type sender:  BaseActor
        """
        self.payload = payload
        self.sender = sender

    def __repr__(self):
        """
        Get a string representation of the message
        """
        if self.payload:
            return "Message ({})".format(self.payload)
        else:
            return "Message()"


class QueryMessage(Message):
    """
    A query message with a global result var
    """
    result = None


class Tick(Message):
    pass


class RegisterTime(Message):
    pass


class Broadcast(Message):
    pass


class PoisonPill(Message):
    pass


class RouteTell(Message):
    pass


class RouteAsk(Message):
    pass


class RouteBroadcast(Message):
    pass


class Pull(Message):
    pass


class Publish(Message):
    pass


class Demand(Message):
    pass


class Subscribe(Message):
    pass


class DeSubscribe(Message):
    pass


class FlowResult(Message):
    pass


class SetAccountant(Message):
    pass
