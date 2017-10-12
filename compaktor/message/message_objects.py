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


class TaskMessage(Message):
    pass


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

class SplitSubscribe(Message):

    def __init__(self, split_name, payload, sender=None):
        """
        Constructor

        :param split_name: The name of the pubsub split
        :type split_name: str()
        :param payload: The message to send containing the actor
        :type payload: AbstractActor()
        :param sender: The sender
        :type sender: AbstractActor()
        """
        super().__init__(payload, sender)
        self.split_name = split_name

    def __repr__(self):
        return "Message({}, {}, {})".format(self.payload, self.sender,
                                            self.split_name)

    def __str__(self, *args, **kwargs):
        return "Message({}, {}, {})".format(self.payload, self.sender,
                                            self.split_name)


class SplitPublish(Message):
    
    def __init__(self, split_name, payload, sender=None):
        """
        Constructor

        :param split_name: The name of the pubsub split
        :type split_name: str()
        :param payload: The message to send containing the actor
        :type payload: AbstractActor()
        :param sender: The sender
        :type sender: AbstractActor()
        """
        super().__init__(payload, sender)
        self.split_name = split_name

    def __repr__(self):
        return "Message({}, {}, {})".format(self.payload, self.sender,
                                            self.split_name)

    def __str__(self, *args, **kwargs):
        return "Message({}, {}, {})".format(self.payload, self.sender,
                                            self.split_name)


class SplitPull(Message):
    
    def __init__(self, split_name, payload, sender=None):
        """
        Constructor

        :param split_name: The name of the pubsub split
        :type split_name: str()
        :param payload: The message to send containing the actor
        :type payload: AbstractActor()
        :param sender: The sender
        :type sender: AbstractActor()
        """
        super().__init__(payload, sender)
        self.split_name = split_name

    def __repr__(self):
        return "Message({}, {}, {})".format(self.payload, self.sender,
                                            self.split_name)

    def __str__(self, *args, **kwargs):
        return "Message({}, {}, {})".format(self.payload, self.sender,
                                            self.split_name)

class SplitDeSubscribe(Message):

    def __init__(self, split_name, payload, sender=None):
        """
        Constructor

        :param split_name: The name of the pubsub split
        :type split_name: str()
        :param payload: The message to send containing the actor
        :type payload: AbstractActor()
        :param sender: The sender
        :type sender: AbstractActor()
        """
        super().__init__(payload, sender)
        self.split_name = split_name

    def __repr__(self):
        return "Message({}, {}, {})".format(self.payload, self.sender,
                                            self.split_name)

    def __str__(self, *args, **kwargs):
        return "Message({}, {}, {})".format(self.payload, self.sender,
                                            self.split_name)