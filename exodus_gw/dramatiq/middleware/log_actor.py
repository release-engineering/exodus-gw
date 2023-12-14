import logging
from contextvars import ContextVar, copy_context
from functools import wraps
from time import monotonic
from typing import Any

from dramatiq import Actor, Middleware
from dramatiq.middleware import CurrentMessage

LOG = logging.getLogger("exodus-gw.actor")


CURRENT_ACTOR: ContextVar[Actor[[], Any]] = ContextVar("CURRENT_ACTOR")
CURRENT_PUBLISH_ID: ContextVar[str] = ContextVar("CURRENT_PUBLISH_ID")
CURRENT_MESSAGE_ID: ContextVar[str] = ContextVar("CURRENT_MESSAGE_ID")


def in_copied_context(fn):
    # Returns a function wrapped to always run in a copy of the
    # current context at time of invocation.
    #
    # This means the function can freely set contextvars without
    # having to worry about resetting them later.
    @wraps(fn)
    def new_fn(*args, **kwargs):
        ctx = copy_context()
        return ctx.run(fn, *args, **kwargs)

    return new_fn


def new_timer():
    # Returns a callable which returns the number of milliseconds
    # passed since new_timer() was called.
    start = monotonic()

    def fn():
        return int((monotonic() - start) * 1000)

    return fn


class ActorFilter(logging.Filter):
    # A filter which will add extra fields onto each log record
    # with info about the currently executing actor.

    def filter(self, record: logging.LogRecord) -> bool:
        if actor := CURRENT_ACTOR.get(None):
            record.actor = actor.actor_name
        if publish_id := CURRENT_PUBLISH_ID.get(None):
            record.publish_id = publish_id
        if message_id := CURRENT_MESSAGE_ID.get(None):
            record.message_id = message_id
        return True


class LogActorMiddleware(Middleware):
    """Middleware to add certain logging behaviors onto all actors."""

    def __init__(self):
        self.filter = ActorFilter()

    def after_process_boot(self, broker):
        logging.getLogger().handlers[0].addFilter(self.filter)

    def before_declare_actor(self, broker, actor):
        old_fn = actor.fn

        @wraps(old_fn)
        def new_fn(*args, **kwargs):
            # Wrapped function sets context vars before execution...
            CURRENT_ACTOR.set(actor)

            if publish_id := kwargs.get("publish_id"):
                CURRENT_PUBLISH_ID.set(publish_id)

            if message := CurrentMessage.get_current_message():
                # This is copied into a contextvar because get_current_message()
                # is a thread-local and not a contextvar, and therefore it
                # wouldn't be available on log records coming from other
                # threads if we don't copy it.
                CURRENT_MESSAGE_ID.set(message.message_id)

            # ...and also ensures some consistent logs appear around
            # the actor invocation: start/stop/error, with timing info.
            timer = new_timer()

            LOG.info("Starting")
            try:
                out = old_fn(*args, **kwargs)
                LOG.info(
                    "Succeeded",
                    extra={"duration_ms": timer(), "success": True},
                )
                return out
            except:
                LOG.warning(
                    "Failed",
                    exc_info=True,
                    extra={"duration_ms": timer(), "success": False},
                )
                raise

        # Make the function run in a copied context so that it can
        # freely set contextvars without having to unset them.
        new_fn = in_copied_context(new_fn)

        actor.fn = new_fn
