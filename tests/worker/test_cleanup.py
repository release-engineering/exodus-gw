import logging
import uuid
from datetime import datetime, timedelta, timezone
from unittest import mock

import pytest
from sqlalchemy.orm.exc import ObjectDeletedError

from exodus_gw.models import CommitTask, Item, Publish, PublishedPath
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

    now = datetime.now(timezone.utc)
    half_day_ago = now - timedelta(hours=12)
    two_days_ago = now - timedelta(days=2)
    eight_days_ago = now - timedelta(days=8)
    thirty_days_ago = now - timedelta(days=30)
    two_years_ago = now - timedelta(days=365 * 2)

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
    path1_old = PublishedPath(
        id=100,
        env="test",
        web_uri="/some/published/path1",
        updated=two_years_ago,
    )
    path2_old = PublishedPath(
        id=101,
        env="test",
        web_uri="/some/published/path2",
        updated=two_years_ago,
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
    path1_recent = PublishedPath(
        id=200,
        env="test",
        web_uri="/some/recently-published/path1",
        updated=thirty_days_ago,
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
            path1_old,
            path2_old,
            p1_recent,
            t1_recent,
            path1_recent,
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
    with pytest.raises(ObjectDeletedError):
        path1_old.id
    with pytest.raises(ObjectDeletedError):
        path2_old.id

    for item in p1_old_items:
        with pytest.raises(ObjectDeletedError):
            item.id

    # Other objects should still exist as they were.
    assert p1_recent.state == PublishStates.pending
    assert t1_recent.state == TaskStates.complete
    assert path1_recent.web_uri == "/some/recently-published/path1"

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
            "PublishedPath ('test', '/some/published/path1'): cleaning old data (last updated: %s)"
            % two_years_ago,
            "PublishedPath ('test', '/some/published/path2'): cleaning old data (last updated: %s)"
            % two_years_ago,
            ####################################################
            # Completed cleanup
            "Scheduled cleanup has completed",
        ]
    )


def test_cleanup_batching(monkeypatch, db):
    """Cleanup processes records in batches, handling more records than
    the batch size."""

    monkeypatch.setenv("EXODUS_GW_CLEANUP_BATCH_SIZE", "10")

    now = datetime.now(timezone.utc)
    thirty_days_ago = now - timedelta(days=30)
    two_years_ago = now - timedelta(days=365 * 2)

    # Create 35 old publishes (will require 4 batches of 10).
    # Some have items to verify items are deleted with their publish.
    old_publishes = []
    old_items = []
    for i in range(35):
        items = []
        if i % 5 == 0:
            items = [
                Item(web_uri=f"/pub{i}/a", object_key="abc", link_to=""),
                Item(web_uri=f"/pub{i}/b", object_key="def", link_to=""),
            ]
            old_items.extend(items)
        p = Publish(
            id=str(uuid.uuid4()),
            env="test",
            state=PublishStates.committed,
            updated=thirty_days_ago,
            items=items,
        )
        old_publishes.append(p)

    # Create 25 old paths (will require 3 batches of 10)
    old_paths = []
    for i in range(25):
        path = PublishedPath(
            id=1000 + i,
            env="test",
            web_uri=f"/old/path/{i}",
            updated=two_years_ago,
        )
        old_paths.append(path)

    # Create some recent records that should NOT be deleted
    recent_publish = Publish(
        id=str(uuid.uuid4()),
        env="test",
        state=PublishStates.committed,
        updated=now - timedelta(hours=1),
    )
    recent_path = PublishedPath(
        id=2000,
        env="test",
        web_uri="/recent/path",
        updated=now - timedelta(days=1),
    )

    db.add_all(old_publishes + old_paths + [recent_publish, recent_path])
    db.commit()

    cleanup()

    db.expire_all()

    # All old publishes and their items should be deleted
    for p in old_publishes:
        with pytest.raises(ObjectDeletedError):
            p.id
    for item in old_items:
        with pytest.raises(ObjectDeletedError):
            item.id

    # All old paths should be deleted
    for path in old_paths:
        with pytest.raises(ObjectDeletedError):
            path.id

    # Recent records should still exist
    assert recent_publish.state == PublishStates.committed
    assert recent_path.web_uri == "/recent/path"


def test_cleanup_interrupted(monkeypatch, db):
    """If cleanup is interrupted (e.g. by TimeLimitExceeded), already-committed
    batches are preserved and remaining records are left for the next run."""

    monkeypatch.setenv("EXODUS_GW_CLEANUP_BATCH_SIZE", "5")

    now = datetime.now(timezone.utc)
    thirty_days_ago = now - timedelta(days=30)

    # Create 20 old publishes
    old_publishes = []
    for i in range(20):
        p = Publish(
            id=str(uuid.uuid4()),
            env="test",
            state=PublishStates.committed,
            updated=thirty_days_ago,
        )
        old_publishes.append(p)

    db.add_all(old_publishes)
    db.commit()

    old_publish_ids = [p.id for p in old_publishes]

    # We'll interrupt after 2 batch commits (10 records deleted).
    # The Janitor calls self.db.commit() after each batch.
    # We patch the Janitor's db.commit to raise after a few calls.
    call_count = 0
    original_commit = None

    from exodus_gw.worker.scheduled import Janitor

    original_init = Janitor.__init__

    def patched_init(self):
        original_init(self)
        original_commit = self.db.commit

        nonlocal call_count

        def counting_commit():
            nonlocal call_count
            # Let fix_timestamps and fix_abandoned commits through (calls 1-2),
            # then let 2 batch commits through (calls 3-4),
            # then raise on the 5th commit (3rd batch).
            call_count += 1
            original_commit()
            if call_count >= 4:
                raise RuntimeError("simulated interruption")

        self.db.commit = counting_commit

    with mock.patch.object(Janitor, "__init__", patched_init):
        with pytest.raises(RuntimeError, match="simulated interruption"):
            cleanup()

    db.expire_all()

    # Count how many old publishes remain
    remaining = (
        db.query(Publish).filter(Publish.id.in_(old_publish_ids)).count()
    )

    # 2 batches of 5 = 10 deleted, 10 should remain
    assert remaining == 10
