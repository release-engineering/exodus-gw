from datetime import datetime, timezone

import dramatiq
from fastapi.testclient import TestClient

from exodus_gw.dramatiq.broker import Broker
from exodus_gw.main import app
from exodus_gw.models import DramatiqConsumer, DramatiqMessage


def test_consume_typical(db):
    """Consuming typical messages from queue works."""

    with TestClient(app):
        broker = Broker()

        @dramatiq.actor(broker=broker)
        def fn1():
            pass

        @dramatiq.actor(broker=broker, priority=10)
        def fn2():
            pass

        fn1.send()
        fn2.send()

        consumer = broker.consume("default", prefetch=3)

        consumer_iter = consumer.__iter__()

        msg1 = next(consumer_iter)
        msg2 = next(consumer_iter)

        # Make it not wait for normal heartbeat interval
        broker.notify()

        msg3 = next(consumer_iter)

    # It should consume those two messages (no particular order)
    assert sorted([msg1.actor_name, msg2.actor_name]) == ["fn1", "fn2"]

    # msg3 should definitely be None since there were only two
    assert not msg3


def test_consumer_lifecycle(db):
    """Consumer maintains a record of itself in the DB."""

    with TestClient(app):
        broker = Broker()
        broker.declare_queue("some-queue")
        consumer = broker.consume("some-queue")

        # Initially we should have no consumers in the DB
        assert db.query(DramatiqConsumer).count() == 0

        # If we start iterating over messages...
        consumer.__iter__()

        # Now this consumer should have recorded itself
        assert db.query(DramatiqConsumer).count() == 1
        consumer_id1 = db.query(DramatiqConsumer).one().id

        # If we close it...
        consumer.close()

        # Then it should have removed itself
        assert db.query(DramatiqConsumer).count() == 0

        # We can make another consumer from the same broker
        consumer = broker.consume("some-queue")
        consumer.__iter__()

        # It should have recorded itself
        assert db.query(DramatiqConsumer).count() == 1
        consumer_id2 = db.query(DramatiqConsumer).one().id

        # And it should have reused the same ID
        assert consumer_id1 == consumer_id2


def test_consumer_immediate_close(db):
    """Consumer can be created and immediately closed without issue, even if DB
    is not yet populated."""

    broker = Broker()
    broker.declare_queue("some-queue")
    consumer = broker.consume("some-queue")

    # It should not crash.
    # The point here is that a consumer would normally remove its own record from
    # the DB, but as we didn't create TestClient, the sqlalchemy tables don't exist
    # in the DB yet. It's important that it should not crash in this case.
    consumer.close()


def test_rescue_dead_messages(db):
    """Consumer automatically detects dead consumers / lost messages and recovers."""

    with TestClient(app):
        broker = Broker()

        @dramatiq.actor(broker=broker)
        def fn1():
            pass

        # Enqueue some messages...
        fn1.send()
        fn1.send()
        fn1.send()

        # Should have made three DB entries
        db_messages = db.query(DramatiqMessage).all()
        assert len(db_messages) == 3

        # Reassign one of them to a nonexistent consumer
        db_messages[0].consumer_id = "this-consumer-does-not-exist"

        # Reassign another to a consumer which has been dead for a long time
        db_messages[1].consumer_id = "this-consumer-is-stale"
        db.add(
            DramatiqConsumer(
                id="this-consumer-is-stale",
                last_alive=datetime(1999, 2, 27, tzinfo=timezone.utc),
            )
        )

        # Reassign another to a consumer which appears to be alive
        db.add(
            DramatiqConsumer(
                id="some-other-consumer", last_alive=datetime.now(timezone.utc)
            )
        )
        db_messages[2].consumer_id = "some-other-consumer"

        # Ensure changes are visible to other connections
        db.commit()

        # Now try to consume messages
        consumer = broker.consume("default", prefetch=3)

        consumer_iter = consumer.__iter__()

        msg1 = next(consumer_iter)
        msg2 = next(consumer_iter)

        # notify so it doesn't sleep
        broker.notify()
        msg3 = next(consumer_iter)

        # Make sure we reload any data changed during the above
        db.expire_all()

        # This consumer should have been able to detect that the first
        # two messages belonged to nonexistent & stale consumers, so
        # could pick them up anyway despite being already assigned.
        db_ids = [m.id for m in db_messages[0:2]]
        consumed_ids = [m.message_id for m in [msg1, msg2]]
        assert sorted(db_ids) == sorted(consumed_ids)

        # DB records similarly should have been updated to point at this
        # consumer
        assert db_messages[0].consumer_id.startswith("default-")
        assert db_messages[1].consumer_id.startswith("default-")

        # However the third message, which is assigned to another consumer
        # which still appears to be alive, should not be consumed
        # consumed
        assert not msg3

        # And the DB record for that message should still belong to the
        # original consumer.
        assert db_messages[2].consumer_id == "some-other-consumer"

        # The stale consumer should also have been cleaned up from DB entirely.
        assert (
            db.query(DramatiqConsumer)
            .filter(DramatiqConsumer.id == "this-consumer-is-stale")
            .count()
            == 0
        )
