from dramatiq import Middleware


class LocalNotifyMiddleware(Middleware):
    """Middleware to notify the broker whenever a message is processed or
    enqueued.

    Note this only achieves local in-process notifies, so it's mainly useful
    in cases where enqueue and consume are happening in the same process,
    for example due to retry middleware or from within tests.
    """

    def after_ack(self, broker, message):
        broker.notify()

    def after_nack(self, broker, message):
        broker.notify()

    def after_enqueue(self, broker, message, delay):
        broker.notify()
