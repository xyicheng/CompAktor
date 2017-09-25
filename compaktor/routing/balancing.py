'''
Created on Sep 21, 2017

@author: aevans
'''


import asyncio
import janus
from compaktor.actor.base_actor import BaseActor
from compaktor.errors.actor_errors import ActorStateError
from compaktor.message.message_objects import RouteAsk,\
    QueryMessage, RouteBroadcast, RouteTell
from compaktor.state.actor_state import ActorState


class BalancingRouter(BaseActor):
    """
    The balancing router uses a single message queue to route messages to
    actors.  Every actor shares the common queue.  This requires reworking
    the actors to allow a special blocking queue.
    """

    def __init__(self, name=None, loop=None, address=None, mailbox_size=10000,
                 inbox=None, actors=[]):
        super().__init__(name, loop, address, mailbox_size, inbox)
        self._queue_base = janus.Queue(maxsize=self.max_inbox_size,
                                       loop=self.loop)
        self._queue = self._queue_base.async_q
        self.actor_set = actors
        self.register_handler(RouteAsk, self.route_ask)
        self.register_handler(RouteTell, self.route_tell)
        self.register_handler(RouteBroadcast, self.attempt_broadcast)
        self.actor_system = None
        self.sys_path = None

    def close_queue(self):
        self._queue_base.close()

    def get_num_actors(self):
        return len(self.actor_set)

    def set_actor_system(self, actor_system, path):
        """
        Set the actor system for the router

        :param actor_system:  The actor system
        :type actor_system:  ActorSystem
        :param path:  The path to add the router to
        :type path:  str
        """
        self.actor_system = actor_system
        self.actor_system.add_actor(self, path)
        self.sys_path = "{}/{}".format(path, self.name)

        for actor in self.actor_set:
            self.actor_system.add_actor(actor, self.sys_path)

    def add_actor(self, actor):
        """
        Add an actor to the ready queue.

        :param actor:  A base actor for the router
        :type actor:  BaseActor
        """
        try:
            if actor.get_state() is ActorState.LIMBO:
                actor.start()
    
            if actor.get_state() is not ActorState.RUNNING:
                raise ActorStateError(
                    "Actor to Add to Round Robin Router Not Working")
    
            if actor not in self.actor_set:
                actor._inbox = self._queue
                self.actor_set.append(actor)
    
            if self.sys_path is not None and self.actor_system is not None:
                self.actor_system.add_actor(actor, self.sys_path)
        except Exception as e:
            self.handle_fail()

    def remove_actor(self, actor):
        """
        Remove an actor from the router.

        :param actor:  The implemented actor to remove
        :type actor:  BaseActor
        """
        try:
            if actor in self.actor_set:
                self.actor_set.remove(actor)
    
                # remove actor from system if set
                if self.sys_path is not None:
                    path = "{}/{}".format(self.sys_path, actor.get_name())
                    self.actor_system.delete_branch(path)
        except Exception as e:
            self.handle_fail()

    async def __handle_remove(self, message):
        """
        Remove an actor from the system.
        """
        try:
            if message:
                actor = message.payload
                if actor:
                    self.remove_actor(actor)
                else:
                    print("Actor to remove was None")
            else:
                print("Message to remove request empty")
        except Exception as e:
            self.handle_fail()
        
        
    async def route_ask(self, message):
        """
        Send an ask request to the queue.  The queue calls must block.
        """
        res = None
        try:
            assert isinstance(message, QueryMessage)
            if not message.result:
                message.result = asyncio.Future(loop=self.loop)
            print("Putting In Queue")
            await self._queue.put(message)
            res = await message.result
        except Exception as e:
            self.handle_fail()
        return res

    async def route_tell(self, message):
        """
        Send a tell request to the queue.  The queue calls on the actor
        should block.
        """
        try:
            await self._queue.put(message)
        except Exception as e:
            self.handle_fail()

    async def attempt_broadcast(self, message):
        """
        'Broadcast' a message to all actors in the router.  This currently
        pushes a message on the queue per actor so broadcast itself is not
        correct.
        """
        try:
            for actor in self.actors:
                self._queue._put(message)
        except Exception as e:
            self.handle_fail()