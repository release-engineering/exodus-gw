import functools

from asgi_correlation_id import correlation_id
from dramatiq import Middleware


class CorrelationIdMiddleware(Middleware):
    """Middleware passing correlation ID from web to workers."""

    def before_declare_actor(self, broker, actor):
        # On the worker side: ensure the actor accepts "correlation_id"
        self.__wrap_with_correlation_id(actor)

    def before_enqueue(self, broker, message, delay):
        # On the web side: ensure that enqueued messages include
        # a "correlation_id".
        message.kwargs["correlation_id"] = correlation_id.get()

    def __wrap_with_correlation_id(self, actor):
        # Wrap actor's callable so that it accepts a "correlation_id" argument
        # which, if provided, is automatically propagated into
        # asgi_correlation_id.correlation_id.
        original_fn = actor.fn

        @functools.wraps(original_fn)
        def new_fn(*args, **kwargs):
            token = None
            if input_id := kwargs.pop("correlation_id", None):
                token = correlation_id.set(input_id)

            try:
                return original_fn(*args, **kwargs)
            finally:
                if token:
                    correlation_id.reset(token)

        actor.fn = new_fn
