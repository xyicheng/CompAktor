'''
Created on Sep 21, 2017

@author: aevans
'''


class ActorStateError(Exception):
    pass


class HandlerNotFoundError(Exception):
    pass


class ChildNotFoundException(Exception):
    pass


class ChildNodeExistsException(Exception):
    pass


class FunctionNotProvidedException(Exception):
    pass


class MissingActorException(Exception):
    pass


class WrongActorException(Exception):
    pass
