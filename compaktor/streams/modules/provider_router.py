'''
Created on Oct 27, 2017

@author: aevans
'''

from compaktor.routing.round_robin import RoundRobinRouter
from compaktor.routing.balancing import BalancingRouter


def create_provider_router(provider_logic, providers):
    """
    Create a provider router.

    :param provider_logic: The router type to create
    :type provider_logic: str()
    """
    router = None
    logic = provider_logic.lower().strip()
    if logic == "round_robin":
        router = RoundRobinRouter()
    elif logic == "balancing":
        router = BalancingRouter()
    else:
        raise ValueError(
            "Can Only Use round_robin or balancing routers in streams")
    router.start()
    return router


def add_provider(router, actor):
    """
    Add a provider to the given router.

    :param router: The router to use
    :type router: RoundRobinRouter() or BalancingRouter()
    :param actor: The actor to add to the router
    :type actor: BaseActor()
    """
    if router and actor:
        router.add_actor(actor)
    else:
        raise ValueError("Router or Actor are None")
