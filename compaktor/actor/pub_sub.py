'''
Created on Aug 30, 2017

@author: aevans
'''


from compaktor.actor.base_actor import BaseActor
from compaktor.errors.actor_errors import HandlerNotFoundError, ActorStateError
from compaktor.message.message_objects import Message, Publish, Subscribe,\
    RouteBroadcast, RouteTell, DeSubscribe
from compaktor.routing.balancing import BalancingRouter
from compaktor.routing.random import RandomRouter
from compaktor.routing.round_robin import RoundRobinRouter
from compaktor.state.actor_state import ActorState


class PubSub(BaseActor):
    """
    Special Publisher/Subscriber for streaming
    """

    def __init__(self, *args, **kwargs):
        """
        The constructor

        :Keyword Arguments:
            *publisher (BaseActor): The publishing actor
            *router (BaseActor): a router for subscribers
        """
        super().__init__(*args, **kwargs)
        self._subscription_router = kwargs.get('router', RoundRobinRouter())
        
        if self._subscription_router.get_state() is ActorState.LIMBO:
            self._subscription_router.start()

        if self._subscription_router.get_state() is not ActorState.RUNNING:
            raise ActorStateError("Router not Started in PubSub")

        self.register_handler(Subscribe, self.subscribe)
        self.register_handler(Publish, self.do_publish)
        self.register_handler(DeSubscribe, self.desubscribe)

    async def subscribe(self, actor):
        """
        Subscribe to the pub/sub.  This will replace the subscriber queue to
        create a balancing router.
        """
        try:
            self._subscription_router.add_actor(actor)
        except Exception as e:
            self.handle_fail()

    async def desubscribe(self, message):
        try:
            await self.tell(self._subscription_router, message)
        except Exception as e:
            self.handle_fail()

    def broadcast(self, message):
        """
        Send a broadcast to all members of the publishers router
        """
        try:
            self.loop.run_until_complete(self.tell(
                self._subscription_router, RouteBroadcast(message)))
        except Exception as e:
            self.handle_fail()

    async def do_publish(self, message):
        """
        Submit a message to a chosen actor in the subscribers
        """
        try:
            await self.tell(self._subscription_router, RouteTell(message.payload))
        except Exception as e:
            self.handle_fail()

    async def handle_broadcast(self, message):
        """
        Broadcast to all actors.  Do not connect the actors subscribing here
        to a broadcast router.
        """
        try:
            for actor in self._subscribers:
                await self.tell(actor, message)
        except Exception as e:
            self.handle_fail()
