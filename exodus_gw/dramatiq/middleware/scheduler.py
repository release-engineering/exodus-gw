import functools
import logging
import uuid
from collections.abc import Callable
from datetime import datetime

import pycron
from dramatiq import Middleware
from dramatiq.common import dq_name
from sqlalchemy import Engine
from sqlalchemy.orm import Session

from exodus_gw.models import DramatiqMessage
from exodus_gw.settings import Settings

LOG = logging.getLogger("exodus-gw")


class SchedulerMiddleware(Middleware):
    """Middleware allowing for scheduled actors with cron-style rules."""

    # Arbitrary constant UUID used as namespace when calculating stable message IDs.
    SCHEDULER_NS = uuid.UUID("71f64e5740d428a533429d81c30e899b")

    def __init__(
        self, settings: Callable[[], Settings], db_engine: Callable[[], Engine]
    ):
        self.__settings = settings
        self.__db_engine = db_engine

    @property
    def actor_options(self):
        return set(["scheduled"])

    def __wrap_with_schedule(self, broker, actor):
        # Wrap actor's callable so that it's invoked according to schedule.
        #
        # Scheduled callable behaves as follows:
        #
        # - a cron-style scheduling rule is looked up as Settings.cron_<actor_name>
        #
        # - whenever the scheduled callable is invoked, it calls the real actor only
        #   if the cron rule has been hit since the last run
        #
        # - whenever the scheduled callable completes, it schedules itself again
        #   after Settings.scheduler_interval
        #
        # Note, scheduled callable doesn't do anything in particular with errors, so
        # those will be processed via the usual retry mechanism.

        settings = self.__settings()

        settings_key = "cron_" + actor.actor_name

        # It is a bug to define a scheduled actor without a corresponding setting.
        assert hasattr(settings, settings_key)

        actor.options["scheduled_message_id"] = str(
            uuid.uuid5(
                self.SCHEDULER_NS,
                "-".join([actor.queue_name, actor.actor_name]),
            )
        )

        LOG.info(
            "Scheduled actor %s uses message %s",
            actor.actor_name,
            actor.options["scheduled_message_id"],
        )

        unscheduled_fn = actor.fn

        # Keep a reference to the original unwrapped function; we do this
        # mainly to make testing easier as it is quite inconvenient to require
        # all tests to set up the current time & cron rule just to invoke
        # an actor.
        actor.options["unscheduled_fn"] = unscheduled_fn

        @functools.wraps(unscheduled_fn)
        def new_fn(last_run: float | None = None):
            cron_rule = getattr(settings, settings_key)
            now = datetime.utcnow()

            if not last_run:
                # If we are being invoked for the first time, there is no obvious
                # 'since' timestamp to use when evaluating the cron rule. We'll
                # pick 30 minutes somewhat arbitrarily.
                last_run = now.timestamp() - 60 * 30

            since = datetime.fromtimestamp(last_run)

            if pycron.has_been(cron_rule, since, now):
                LOG.info(
                    "Scheduled actor %s activated (rule: '%s', period: %s .. %s)",
                    actor.actor_name,
                    cron_rule,
                    since,
                    now,
                )
                unscheduled_fn()
            else:
                # Nothing to do right now, too early for next invoke.
                LOG.debug(
                    "Scheduled actor %s: cron '%s' did not occur within %s .. %s",
                    actor.actor_name,
                    cron_rule,
                    since,
                    now,
                )

            # Call ourselves again soon.
            actor.send_with_options(
                kwargs=dict(last_run=now.timestamp()),
                delay=settings.scheduler_interval * 60 * 1000,
            )

        actor.fn = new_fn

    def before_declare_actor(self, broker, actor):
        if not actor.options.get("scheduled"):
            # nothing to do
            return

        self.__wrap_with_schedule(broker, actor)

    def __ensure_enqueued(self, broker, actor, *args, **kwargs):
        msg = actor.message(*args, **kwargs)

        # Use a fixed message ID for this actor; this ensures there's only one
        # scheduler message in the system for this actor.
        msg = msg.copy(message_id=actor.options["scheduled_message_id"])

        session = Session(bind=self.__db_engine())
        try:
            broker.set_session(session)

            # Enqueue ourselves
            broker.enqueue(msg, delay=Settings().scheduler_delay * 60 * 1000)

            # Clean any other messages to same actor & queue which are NOT this one
            queues = [actor.queue_name, dq_name(actor.queue_name)]
            session.query(DramatiqMessage).filter(
                DramatiqMessage.actor == actor.actor_name,
                DramatiqMessage.queue.in_(queues),
                DramatiqMessage.id != msg.message_id,
            ).delete(synchronize_session=False)

            # And commit
            session.commit()
        finally:
            session.close()
            broker.set_session(None)

    def after_process_boot(self, broker):
        for actor_name in broker.get_declared_actors():
            actor = broker.get_actor(actor_name)
            if actor.options.get("scheduled"):
                self.__ensure_enqueued(broker, actor)
