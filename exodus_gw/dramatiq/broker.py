import logging
import uuid
from collections import defaultdict
from contextvars import ContextVar
from threading import Event

import dramatiq
from dramatiq.common import current_millis, dq_name
from dramatiq.middleware import CurrentMessage
from sqlalchemy.orm import Session

from exodus_gw.database import db_engine
from exodus_gw.dramatiq.consumer import Consumer
from exodus_gw.dramatiq.middleware import (
    CorrelationIdMiddleware,
    DatabaseReadyMiddleware,
    LocalNotifyMiddleware,
    LogActorMiddleware,
    PostgresNotifyMiddleware,
    SchedulerMiddleware,
    SettingsMiddleware,
)
from exodus_gw.logging import loggers_init
from exodus_gw.models import DramatiqMessage
from exodus_gw.settings import load_settings

LOG = logging.getLogger("exodus-gw")


class Broker(dramatiq.Broker):  # pylint: disable=abstract-method
    """Dramatiq broker using sqlalchemy to process messages."""

    def __init__(self, middleware=None, settings=None):
        super().__init__(middleware=middleware)

        self.__settings = settings or load_settings()
        self.__db_engine = db_engine(self.__settings)
        self.__shared_session = ContextVar("shared_session")
        self.__broker_id = uuid.uuid4()
        self.__queue_events = defaultdict(Event)

        loggers_init(self.__settings)

        # In the middlewares added below, references to settings and DB engine
        # are kept indirect so that it's possible to reset these attributes on
        # the broker and ensure they take effect.
        get_settings = lambda: self.__settings
        get_db_engine = lambda: self.__db_engine

        # We have some actors using this, so it's always enabled.
        self.add_middleware(CurrentMessage())

        # Enable special handling of actors with 'scheduled=True' in options.
        self.add_middleware(SchedulerMiddleware(get_settings, get_db_engine))

        # Enable automatic prefixing of log messages with actor names/identity
        # such as "[commit <publish-id>] the log message..."
        self.add_middleware(LogActorMiddleware())

        # Allow correlation ID to propagate between web and worker
        self.add_middleware(CorrelationIdMiddleware())

        # Ensure all actors can get access to the current settings.
        self.add_middleware(SettingsMiddleware(get_settings))

        self.add_middleware(LocalNotifyMiddleware())

        # Enable Database readycheck when booting up a worker.
        self.add_middleware(DatabaseReadyMiddleware(get_db_engine))

        # Enable postgres notify/listen.
        # This middleware checks internally whether postgres is used.
        self.add_middleware(PostgresNotifyMiddleware(get_db_engine))

    def reset(self):
        """Reset the broker, reinitializing settings and DB engine.

        Intended for usage only during tests, as a way of forcing the existing
        broker to point at a clean database.
        """
        self.__settings = load_settings()
        self.__db_engine = db_engine(self.__settings)
        self.set_session(None)

    def set_session(self, session):
        """Set an sqlalchemy session for use with the broker.

        A session should be set during handling of any HTTP requests.
        It will ensure that enqueues take place in the same transaction
        as other changes made during handling of that request.

        In other contexts, e.g. one dramatiq actor invoking another,
        the session can be safely left unset and the broker will manage
        its own session as needed.
        """
        self.__shared_session.set(session)

    @property
    def session(self):
        return self.__shared_session.get(None)

    def notify(self):
        """Notify all consumers that something might have changed.

        Consumers are called in a loop and they will sleep between iterations.
        Calling this method will wake any sleeping consumers so that new messages
        can be found earlier.
        """
        queues = self.get_declared_queues().union(
            self.get_declared_delay_queues()
        )
        for queue_name in queues:
            self.__queue_events[queue_name].set()

    def declare_queue(self, queue_name):
        if queue_name not in self.queues:
            self.emit_before("declare_queue", queue_name)
            self.queues[queue_name] = None
            self.emit_after("declare_queue", queue_name)

            delayed_name = dq_name(queue_name)
            self.queues[delayed_name] = None
            self.delay_queues.add(delayed_name)
            self.emit_after("declare_delay_queue", delayed_name)

    def consume(self, queue_name, prefetch=1, timeout=30000):
        consumer_id = "%s-%s" % (queue_name, self.__broker_id)

        # We need one (arbitrarily selected) consumer to act as the
        # "master" which will take on additional maintenance duties.
        # We'll have one consumer per queue and it would be wasteful
        # to let all of them do this.
        master = queue_name == list(self.queues.keys())[0]

        return Consumer(
            queue_name,
            db_engine=self.__db_engine,
            consumer_id=consumer_id,
            prefetch=prefetch,
            master=master,
            queue_event=self.__queue_events[queue_name],
            settings=self.__settings,
        )

    def enqueue_using_session(self, db, message, delay=None):
        # Given a dramatiq message, saves it to the queue in the DB.
        queue_name = message.queue_name

        if delay is not None:
            queue_name = dq_name(queue_name)
            message.options["eta"] = current_millis() + delay

        db_message = DramatiqMessage(
            id=message.message_id, actor=message.actor_name, queue=queue_name
        )

        message_dict = message.asdict()

        # Drop these so we're not storing them in two places.
        del message_dict["message_id"]
        del message_dict["queue_name"]
        del message_dict["actor_name"]

        db_message.body = message_dict

        # Use merge rather than add since the message may already exist;
        # for instance this happens in case of retry.
        db_message = db.merge(db_message)

        # Explicitly wipe out the consumer, since if we've updated an existing
        # message it'll have to be consumed again.
        db_message.consumer_id = None

    def enqueue(self, message, *, delay=None):
        self.emit_before("enqueue", message, delay)

        if db := self.session:
            # We have a shared session, e.g. we're in an http request handler.
            # We reuse the session and don't commit ourselves.
            self.enqueue_using_session(db, message, delay)
            self.emit_after("enqueue", message, delay)
            return message

        # We don't have a shared session e.g.
        # - an automated test not using a real app
        # - testing from python CLI
        # - one actor invoking another
        # Then we use our own short-lived session and commit.
        db = Session(bind=self.__db_engine)
        try:
            self.enqueue_using_session(db, message, delay)
            db.commit()
            self.emit_after("enqueue", message, delay)
        finally:
            db.close()

        return message
