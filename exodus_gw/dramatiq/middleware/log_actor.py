import logging
from functools import wraps
from threading import local

from dramatiq import Middleware


class PrefixFilter(logging.Filter):
    # A filter which will add an arbitrary thread-local prefix onto each message.

    def __init__(self):
        self.tls = local()
        super().__init__()

    def filter(self, record: logging.LogRecord) -> bool:
        record.msg = getattr(self.tls, "prefix", "") + record.msg
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
        # PrefixFilter's prefix around the function's invocation.

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

            try:
                self.filter.tls.prefix = prefix
                return fn(*args, **kwargs)
            finally:
                self.filter.tls.prefix = ""

        return new_fn
