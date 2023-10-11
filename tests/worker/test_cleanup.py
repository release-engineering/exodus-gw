import logging
import uuid
from datetime import datetime, timedelta

import pytest
from sqlalchemy.orm.exc import ObjectDeletedError

from exodus_gw.models import CommitTask, Item, Publish
from exodus_gw.schemas import PublishStates, TaskStates
from exodus_gw.worker import cleanup

# cleanup is a scheduled actor, we want to test the actor itself
# and not the scheduling mechanism, so unwrap it to get at the
# implementation.

cleanup = cleanup.options["unscheduled_fn"]


def test_cleanup_noop(caplog, db):
    """Cleanup with no data to clean does nothing, successfully."""

    logging.getLogger("exodus-gw").setLevel(logging.INFO)

    # Should run
    cleanup()

    # It should not have done anything, other than log this.
    messages = [
        record.message
        for record in caplog.records
        if record.name == "exodus-gw"
    ]
    assert messages == ["Scheduled cleanup has completed"]


def test_cleanup_mixed(caplog, db):
    """Cleanup manipulates objects in the expected manner."""

    logging.getLogger("exodus-gw").setLevel(logging.INFO)

    # Note: datetimes in this test assume the default timeout values
    # are used.

    now = datetime.utcnow()
    half_day_ago = now - timedelta(hours=12)
    two_days_ago = now - timedelta(days=2)
    eight_days_ago = now - timedelta(days=8)
    thirty_days_ago = now - timedelta(days=30)

    # Some objects with missing timestamps.
    p1_missing_ts = Publish(
        id=str(uuid.uuid4()),
        env="test",
        state=PublishStates.failed,
        updated=None,
    )
    p2_missing_ts = Publish(
        id=str(uuid.uuid4()),
        env="test2",
        state=PublishStates.committed,
        updated=None,
    )
    t1_missing_ts = CommitTask(
        id=str(uuid.uuid4()),
        publish_id=p1_missing_ts.id,
        state=TaskStates.failed,
        updated=None,
    )

    # Some publishes which seem to be abandoned.
    p1_abandoned = Publish(
        id=str(uuid.uuid4()),
        env="test",
        state=PublishStates.pending,
        updated=eight_days_ago,
        items=[
            Item(web_uri="/1", object_key="abc", link_to=""),
            Item(web_uri="/2", object_key="aabbcc", link_to=""),
        ],
    )
    p2_abandoned = Publish(
        id=str(uuid.uuid4()),
        env="test",
        state=PublishStates.committing,
        updated=thirty_days_ago,
    )
    t1_abandoned = CommitTask(
        id=str(uuid.uuid4()),
        publish_id=p1_abandoned.id,
        state=TaskStates.in_progress,
        updated=eight_days_ago,
    )

    # Some objects which are old enough to be cleaned up.
    p1_old = Publish(
        id=str(uuid.uuid4()),
        env="test2",
        state=PublishStates.committed,
        updated=thirty_days_ago,
        items=[
            Item(web_uri="/1", object_key="abc", link_to=""),
            Item(web_uri="/2", object_key="aabbcc", link_to=""),
        ],
    )
    p2_old = Publish(
        id=str(uuid.uuid4()),
        env="test3",
        state=PublishStates.failed,
        updated=thirty_days_ago,
    )
    t1_old = CommitTask(
        id=str(uuid.uuid4()),
        publish_id=p1_old.id,
        state=TaskStates.failed,
        updated=thirty_days_ago,
    )
    # (Because these objects will be deleted, we need to keep their ids separately.)
    p1_old_id = p1_old.id
    p2_old_id = p2_old.id
    t1_old_id = t1_old.id
    p1_old_items = p1_old.items

    # And finally some recent objects which should not be touched at all.
    p1_recent = Publish(
        id=str(uuid.uuid4()),
        env="test3",
        state=PublishStates.pending,
        updated=half_day_ago,
    )
    t1_recent = CommitTask(
        id=str(uuid.uuid4()),
        publish_id=p1_recent.id,
        state=TaskStates.complete,
        updated=two_days_ago,
    )

    db.add_all(
        [
            p1_missing_ts,
            p2_missing_ts,
            t1_missing_ts,
            p1_abandoned,
            p2_abandoned,
            t1_abandoned,
            p1_old,
            p2_old,
            t1_old,
            p1_recent,
            t1_recent,
        ]
    )
    db.commit()

    # Should run successfully
    cleanup()

    # Make sure we reload anything which has changed
    db.expire_all()

    # Missing timestamps should now be filled in (fuzzy comparison as exact
    # time is not set)
    assert (t1_missing_ts.updated - now) < timedelta(seconds=10)
    assert (p1_missing_ts.updated - now) < timedelta(seconds=10)
    assert (p2_missing_ts.updated - now) < timedelta(seconds=10)

    # The abandoned objects should now be marked as failed
    assert p1_abandoned.state == PublishStates.failed
    assert p2_abandoned.state == PublishStates.failed
    assert t1_abandoned.state == TaskStates.failed

    # The old objects should no longer exist.
    with pytest.raises(ObjectDeletedError):
        p1_old.id
    with pytest.raises(ObjectDeletedError):
        p2_old.id
    with pytest.raises(ObjectDeletedError):
        t1_old.id

    for item in p1_old_items:
        with pytest.raises(ObjectDeletedError):
            item.id

    # Other objects should still exist as they were.
    assert p1_recent.state == PublishStates.pending
    assert t1_recent.state == TaskStates.complete

    # It should have logged exactly what it did.
    messages = [
        record.message
        for record in caplog.records
        if record.name == "exodus-gw"
    ]
    assert sorted(messages) == sorted(
        [
            ####################################################
            # Fixed timestamps
            "Task %s: setting updated" % (t1_missing_ts.id,),
            "Publish %s: setting updated" % (p1_missing_ts.id,),
            "Publish %s: setting updated" % (p2_missing_ts.id,),
            ####################################################
            # Abandoned objects
            "Task %s: marking as failed (last updated: %s)"
            % (t1_abandoned.id, eight_days_ago),
            "Publish %s: marking as failed (last updated: %s)"
            % (p1_abandoned.id, eight_days_ago),
            "Publish %s: marking as failed (last updated: %s)"
            % (p2_abandoned.id, thirty_days_ago),
            ####################################################
            # Deleted old stuff
            "Task %s: cleaning old data (last updated: %s)"
            % (t1_old_id, thirty_days_ago),
            "Publish %s: cleaning old data (last updated: %s)"
            % (p1_old_id, thirty_days_ago),
            "Publish %s: cleaning old data (last updated: %s)"
            % (p2_old_id, thirty_days_ago),
            ####################################################
            # Completed cleanup
            "Scheduled cleanup has completed",
        ]
    )
