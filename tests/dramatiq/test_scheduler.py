import uuid
from datetime import datetime
from unittest import mock

import dramatiq
import pytest

from exodus_gw.dramatiq import Broker
from exodus_gw.models import DramatiqMessage
from exodus_gw.settings import Settings


class ExtendedSettings(Settings):
    cron_schedule_test1: str = ""
    cron_schedule_test2: str = ""


@pytest.fixture
def mock_utcnow():
    with mock.patch(
        "exodus_gw.dramatiq.middleware.scheduler.datetime"
    ) as mock_datetime:
        mock_datetime.fromtimestamp.side_effect = datetime.fromtimestamp
        yield mock_datetime.utcnow


def test_scheduled_actor_invoked_on_rule_match(db, mock_utcnow):
    """Scheduled actors only get invoked when cron rule from settings is matched."""

    settings = ExtendedSettings()
    settings.cron_schedule_test1 = "5 1,2,3 * * *"
    broker = Broker(settings=settings)

    calls = []

    @dramatiq.actor(scheduled=True, broker=broker)
    def schedule_test1():
        calls.append(True)

    now = datetime(1999, 1, 1)
    mock_utcnow.return_value = now

    # First call does not match the cron rule
    schedule_test1.fn()

    # So it shouldn't trigger the underlying actor
    assert not calls

    # Set time to 1:07, just after 1:05 hitting the cron rule
    now = now.replace(hour=1, minute=7)
    mock_utcnow.return_value = now

    # Now it should hit the rule if looking back a few minutes
    schedule_test1.fn(now.timestamp() - 120.0)
    assert len(calls) == 1

    # While looking back 30 seconds should not
    schedule_test1.fn(now.timestamp() - 30.0)
    assert len(calls) == 1

    # Same at 3:07
    now = now.replace(hour=3)
    mock_utcnow.return_value = now
    schedule_test1.fn(now.timestamp() - 120.0)
    assert len(calls) == 2

    # But no match at 4:07
    now = now.replace(hour=4)
    mock_utcnow.return_value = now
    schedule_test1.fn(now.timestamp() - 120.0)
    assert len(calls) == 2


def test_scheduled_actor_enqueued_on_startup(db):
    """Scheduled actors automatically get enqueued/reset when broker boots."""

    settings = ExtendedSettings()
    settings.cron_schedule_test1 = "5 1,2,3 * * *"
    settings.cron_schedule_test2 = "5 1,2,3 * * *"
    broker = Broker(settings=settings)

    @dramatiq.actor(scheduled=True, broker=broker)
    def schedule_test1():
        pass

    @dramatiq.actor(scheduled=True, broker=broker)
    def schedule_test2():
        pass

    # Simulate that some messages already exist when
    # broker starts up.
    db.add(
        DramatiqMessage(
            id=str(uuid.uuid4()),
            queue="default",
            actor="schedule_test1",
            body={},
        )
    )
    db.add(
        DramatiqMessage(
            id=str(uuid.uuid4()),
            queue="default",
            actor="schedule_test1",
            body={},
        )
    )
    db.add(
        DramatiqMessage(
            id=str(uuid.uuid4()),
            queue="default",
            actor="schedule_test2",
            body={},
        )
    )

    # There can be messages for unrelated actors too.
    db.add(
        DramatiqMessage(
            id="f3d9ae4a-eea6-946a-8668-445395ba10b7",
            queue="default",
            actor="other_actor",
            body={},
        )
    )

    db.commit()

    # Boot up the broker.
    broker.emit_after("process_boot")

    # Now check DB state:
    db.expire_all()
    db_messages = sorted(
        db.query(DramatiqMessage).all(), key=lambda msg: msg.actor
    )

    raw_messages = [(str(m.id), m.queue, m.actor) for m in db_messages]

    # During boot, it should clean existing scheduled messages and
    # ensure exactly one message per scheduled actor, leaving us with
    # three messages:
    assert raw_messages == [
        # The message unrelated to scheduler is left untouched
        ("f3d9ae4a-eea6-946a-8668-445395ba10b7", "default", "other_actor"),
        # Then one message per scheduled actor.
        # Note: messages go to delayed queue (DQ) since they are delayed messages.
        # Note: it is correct that UUIDs are hardcoded here even though random
        # ones were used above. The system uses a fixed UUID for the initial
        # message per scheduled actor.
        (
            "b343626c-8d3a-50b3-9624-f18e61020dce",
            "default.DQ",
            "schedule_test1",
        ),
        (
            "dbe98881-cac5-5446-9dd9-1912cae1c71c",
            "default.DQ",
            "schedule_test2",
        ),
    ]
