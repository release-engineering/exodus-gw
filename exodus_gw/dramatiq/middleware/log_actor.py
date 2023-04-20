import logging
from contextvars import ContextVar
from functools import wraps

from dramatiq import Middleware

CURRENT_PREFIX: ContextVar[str] = ContextVar("CURRENT_PREFIX")


class PrefixFilter(logging.Filter):
    # A filter which will add CURRENT_PREFIX onto each message.

    def filter(self, record: logging.LogRecord) -> bool:
        record.msg = CURRENT_PREFIX.get("") + record.msg
        return True


class LogActorMiddleware(Middleware):
    """Middleware to prefix every log message with the current actor name."""

    def __init__(self):
        self.filter = PrefixFilter()

    def after_process_boot(self, broker):
        logging.getLogger().handlers[0].addFilter(self.filter)

    def before_declare_actor(self, broker, actor):
        actor.fn = self.wrap_fn_with_prefix(actor.fn)

    def wrap_fn_with_prefix(self, fn):
        # Given a function, returns a wrapped version of it which will adjust
        # CURRENT_PREFIX around the function's invocation.

        @wraps(fn)
        def new_fn(*args, **kwargs):
            # We want to show the function name (which is the actor name)...
            prefix = fn.__name__

            # If the actor takes a publish or task ID as an argument, we want
            # to show that as well
            for key in ("publish_id", "task_id"):
                if key in kwargs:
                    prefix = f"{prefix} {kwargs[key]}"
                    break

            prefix = f"[{prefix}] "

            token = CURRENT_PREFIX.set(prefix)
            try:
                return fn(*args, **kwargs)
            finally:
                CURRENT_PREFIX.reset(token)

        return new_fn
