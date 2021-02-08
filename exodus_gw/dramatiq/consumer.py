import logging
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta

import dramatiq
from dramatiq import Message, MessageProxy
from sqlalchemy.orm import Session

from exodus_gw.models import DramatiqConsumer, DramatiqMessage
from exodus_gw.settings import Settings

LOG = logging.getLogger("exodus-gw")


class Consumer(dramatiq.Consumer):
    """Consumer which keeps track of messages in a table using
    sqlalchemy.
    """

    def __init__(
        self,
        queue_name,
        db_engine,
        queue_event,
        consumer_id=None,
        prefetch=1,
        master=False,
        settings=None,
    ):
        self.__queue_name = queue_name
        self.__db_engine = db_engine
        self.__consumer_id = consumer_id or uuid.uuid4()
        self.__prefetch = prefetch
        self.__master = master
        self.__settings = settings or Settings()
        self.__queue_event = queue_event
        self.__last_heartbeat = 0
        self.__last_consume = 0
        self.__started = False

    # Helper for scoped session.
    # Note: this can be refactored with sqlalchemy >= 14 which supports
    # 'with' statements natively.
    @contextmanager
    def __db_session(self):
        session = Session(bind=self.__db_engine)
        try:
            yield session
        finally:
            session.close()

    def __heartbeat(self):
        # Function invoked regularly for consumer maintenance
        if (
            time.monotonic() - self.__last_heartbeat
            < self.__settings.worker_keepalive_interval
        ):
            # Too early for next heartbeat
            return

        with self.__db_session() as db:
            # Mark ourselves as still alive
            db_consumer = (
                db.query(DramatiqConsumer)
                .filter(DramatiqConsumer.id == self.__consumer_id)
                .one()
            )
            db_consumer.last_alive = datetime.utcnow()

            # Master also performs additional maintenance:
            if self.__master:
                # Remove any timed out consumers
                self.__clean_dead_consumers(db)

                # Reset any messages belonging to nonexistent consumers so they
                # can be picked up again.
                self.__reset_lost_messages(db)

            # Commit these changes
            db.commit()

            self.__last_heartbeat = time.monotonic()

    def __reset_lost_messages(self, db):
        # Reset any messages belonging to nonexistent consumers so that they
        # can be picked up again by an alive consumer.
        lost_messages = (
            db.query(DramatiqMessage)
            .with_for_update(of=DramatiqMessage)
            .outerjoin(DramatiqMessage.consumer)
            .filter(DramatiqMessage.consumer_id != None)
            .filter(DramatiqConsumer.id == None)
        )
        for message in lost_messages:
            # This means we have a message with non-null consumer_id,
            # but no consumer having that ID.
            LOG.warning("Resetting lost message %s", message.id)
            message.consumer_id = None

    def __clean_dead_consumers(self, db):
        timeout = datetime.utcnow() - timedelta(
            seconds=self.__settings.worker_keepalive_timeout
        )

        # We do (query + delete) rather than just delete for logging purposes.
        dead_consumers = db.query(DramatiqConsumer).filter(
            DramatiqConsumer.last_alive <= timeout
        )
        for dead_consumer in dead_consumers:
            LOG.warning(
                "Removing dead consumer %s (last alive %s)",
                dead_consumer.id,
                dead_consumer.last_alive,
            )
            db.delete(dead_consumer)

    def __iter__(self):
        # We're starting up, add a record of ourselves to the DB.
        with self.__db_session() as db:
            db_consumer = DramatiqConsumer(
                id=self.__consumer_id, last_alive=datetime.utcnow()
            )
            db.add(db_consumer)
            db.commit()
            self.__started = True
            LOG.info("%s: consumer is running", self.__consumer_id)

        return self

    def __consume_one(self, db):
        # First check if we're still allowed to fetch more.
        have_count = (
            db.query(DramatiqMessage)
            .filter(DramatiqMessage.consumer_id == self.__consumer_id)
            .count()
        )
        if have_count >= self.__prefetch:
            LOG.debug(
                "Too many pending messages (%s), not consuming more",
                have_count,
            )
            return

        # Take any message in the queue not yet assigned to a consumer.
        db_message = (
            db.query(DramatiqMessage)
            .with_for_update()
            .filter(DramatiqMessage.consumer_id == None)
            .filter(DramatiqMessage.queue == self.__queue_name)
            .first()
        )

        if db_message:
            LOG.info("%s: consumed %s", self.__consumer_id, db_message.id)

            body = db_message.body
            out = Message(
                queue_name=db_message.queue,
                actor_name=db_message.actor,
                message_id=db_message.id,
                **body,
            )

            # Mark it as ours.
            db_message.consumer_id = self.__consumer_id

            return MessageProxy(out)

        LOG.debug("%s: did not find any messages", self.__consumer_id)

    def __try_consume(self):
        # Consume one message if enough time has passed since the last one.
        now = time.monotonic()
        if (
            now - self.__last_consume
            < self.__settings.worker_keepalive_interval
            and not self.__queue_event.is_set()
        ):
            return

        self.__last_consume = now

        with self.__db_session() as db:
            message = self.__consume_one(db)
            if message:
                db.commit()
                return message

        # Event is only cleared if we didn't find anything, because if
        # we *did* find something then it's appropriate to recheck again ASAP
        # since there could be more messages.
        self.__queue_event.clear()

    def __next__(self):
        # dramatiq calls this method repeatedly to get messages.

        # let everyone know we're still alive
        self.__heartbeat()

        # get a message if we can
        if out := self.__try_consume():
            return out

        # Nothing to consume, wait a second, or possibly less if we're notified.
        self.__queue_event.wait(1.0)

    def ack(self, message):
        if "eta" in message.options:
            # This is a delayed message - we should not do anything on ack
            # since it's not really executed yet. We will be called again
            # after the delayed message is converted to non-delayed message.
            return

        # For a regular message, ACK means we're done with it, so delete it.
        with self.__db_session() as db:
            db.query(DramatiqMessage).filter(
                DramatiqMessage.id == message.message_id
            ).delete()
            db.commit()

            LOG.info(
                "%s: ACK %s",
                self.__consumer_id,
                message.message_id,
            )

    def nack(self, message):
        # Called when a message failed (after all retries exhausted).
        #
        # We'll clean it up, and can't really do anything else except log
        # an error.
        with self.__db_session() as db:
            db.query(DramatiqMessage).filter(
                DramatiqMessage.id == message.message_id
            ).delete()
            db.commit()

            LOG.error(
                "%s: message failed: %s\n%s",
                self.__consumer_id,
                message.message_id,
                # Dump whole message in log so there's a permanent record
                message.asdict(),
            )

    def close(self):
        if not self.__started:
            # Bail out early if we're closing before started, because the
            # reason we're closing *could* be that the worker was started
            # before the web app has run migrations. If so, we should not
            # try to use the DB here.
            return

        # We're shutting down, clean up our record.
        LOG.info("%s: closing", self.__consumer_id)

        with self.__db_session() as db:
            db.query(DramatiqConsumer).filter(
                DramatiqConsumer.id == self.__consumer_id
            ).delete()
            db.commit()
