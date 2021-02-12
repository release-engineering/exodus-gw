import dramatiq
from dramatiq.middleware import CurrentMessage

from .broker import new_broker

broker = new_broker()
broker.add_middleware(CurrentMessage())
dramatiq.set_broker(broker)

from .publish import commit  # noqa


@dramatiq.actor(store_results=True, priority=100)
def ping():
    """A trivial actor used for healthcheck purposes.

    This can be invoked to demonstrate that workers are running
    and able to pass results back to the caller.
    """
    return "pong"
