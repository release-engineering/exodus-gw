import inspect
from collections.abc import Callable
from functools import wraps

from dramatiq import Middleware

from exodus_gw.settings import Settings


class SettingsMiddleware(Middleware):
    """Middleware to make a Settings object available to all actors."""

    def __init__(self, settings: Callable[[], Settings]):
        self.__settings = settings

    def before_declare_actor(self, broker, actor):
        original_fn = actor.fn
        sig = inspect.signature(original_fn)

        if "settings" in sig.parameters:

            @wraps(original_fn)
            def new_fn(*args, **kwargs):
                # Settings are automatically injected if there is no
                # value present.
                # If a value is present, it's not overwritten; this allows
                # calling actors with specific settings during tests.
                if not kwargs.get("settings"):
                    kwargs["settings"] = self.__settings()
                return original_fn(*args, **kwargs)

            actor.fn = new_fn
