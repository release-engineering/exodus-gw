import inspect
from functools import partial, update_wrapper

from dramatiq import Middleware


class SettingsMiddleware(Middleware):
    """Middleware to make a Settings object available to all actors."""

    def __init__(self, settings):
        self.__settings = settings

    def before_declare_actor(self, broker, actor):
        sig = inspect.signature(actor.fn)

        if "settings" in sig.parameters:
            new_fn = partial(actor.fn, settings=self.__settings)
            update_wrapper(new_fn, actor.fn)
            actor.fn = new_fn
