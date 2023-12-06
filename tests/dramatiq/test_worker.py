"""These tests do system testing of our broker & consumer by running a real worker."""

import contextvars
import logging
import os
import threading
import time

import dramatiq
import pytest
from asgi_correlation_id import correlation_id
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from exodus_gw.dramatiq.broker import Broker
from exodus_gw.main import app
from exodus_gw.models import CommitTask, DramatiqMessage, Publish

# How many worker threads we use.
# Some tests need to know this.
THREAD_COUNT = 4


def assert_soon(callable, timeout=5):
    # helper to assert something which becomes true asynchronously
    while True:
        try:
            assert callable()
            return
        except AssertionError:
            if timeout < 0:
                raise
            timeout -= 0.1
            time.sleep(0.1)


class Actors:
    # collection of actors plus tracking for their calls
    def __init__(self, broker):
        self.blocking_sem = threading.Semaphore(0)
        self.actor_calls = []
        self.actor_call_times = []

        # note we wrap these as actors here rather than using decorators so that
        # we can wait until the test creates a broker instance
        self.basic = dramatiq.actor(self.basic, broker=broker)
        self.basic_other_queue = dramatiq.actor(
            self.basic_other_queue, queue_name="other-queue", broker=broker
        )
        self.always_fails = dramatiq.actor(
            self.always_fails, broker=broker, max_backoff=0.001, max_retries=1
        )
        self.always_fails_blocking = dramatiq.actor(
            self.always_fails_blocking,
            broker=broker,
            max_backoff=0.001,
            max_retries=1,
        )
        self.succeeds_on_retry = dramatiq.actor(
            self.succeeds_on_retry, broker=broker, max_backoff=0.001
        )
        self.blocking_fn = dramatiq.actor(self.blocking_fn, broker=broker)
        self.log_warning = dramatiq.actor(self.log_warning, broker=broker)
        self.log_warning_from_thread = dramatiq.actor(
            self.log_warning_from_thread, broker=broker
        )

    def record_call(self, name):
        self.actor_calls.append(name)
        self.actor_call_times.append(time.monotonic())

    def reset(self):
        self.actor_calls = []
        self.actor_call_times = []
        for _ in range(0, 100):
            self.blocking_sem.release()
        self.blocking_sem = threading.Semaphore(0)

    def basic(self, x):
        self.record_call("basic")

    def basic_other_queue(self, x):
        self.record_call("basic_other_queue")

    def always_fails(self):
        self.record_call("always_fails")
        raise RuntimeError("I always fail!")

    def always_fails_blocking(self):
        self.blocking_sem.acquire()
        self.record_call("always_fails_blocking")
        raise RuntimeError("I always fail (once I'm unblocked)!")

    def succeeds_on_retry(self):
        should_fail = "succeeds_on_retry" not in self.actor_calls
        self.record_call("succeeds_on_retry")
        if should_fail:
            raise RuntimeError("I sometimes fail!")

    def blocking_fn(self):
        self.record_call("blocking_fn start")
        self.blocking_sem.acquire()
        self.record_call("blocking_fn end")

    def log_warning(self, task_id, value):
        self.record_call("log_warning")
        logging.getLogger("any-logger").warning(
            "warning from actor: %s", value
        )

    def log_warning_from_thread(self, task_id, value):
        self.record_call("log_warning_from_thread")

        def fn_in_thread():
            logging.getLogger("any-logger").warning(
                "warning from actor: %s", value
            )

        # Run the logging statement from within a thread which
        # is set up to propagate the current context. We want to
        # verify that the log prefix goes along with the context.
        context = contextvars.copy_context()
        thread = threading.Thread(target=context.run, args=(fn_in_thread,))
        thread.start()
        thread.join()


# start/stop of worker is relatively slow, hence why we use module-scoped
# fixtures here so we are not repeatedly starting/stopping a worker for all
# the cases.


@pytest.fixture(scope="module")
def db_url(tmpdir_factory):
    # In this test, since we're using module-scoped fixtures,
    # we'll use our own separate DB to avoid messing with any other tests.

    return "sqlite:///%s" % tmpdir_factory.mktemp("exodus-gw-test").join(
        "broker-test.db"
    )


@pytest.fixture(scope="module")
def broker(db_url):
    os.environ["EXODUS_GW_DB_URL"] = db_url

    with TestClient(app):
        broker = Broker()
        broker.emit_after("process_boot")
        yield broker

    del os.environ["EXODUS_GW_DB_URL"]


@pytest.fixture()
def db(db_url):
    session = Session(bind=create_engine(db_url))
    try:
        yield session
    finally:
        session.close()


@pytest.fixture(scope="module")
def session_actors(broker):
    return Actors(broker)


@pytest.fixture(scope="module")
def worker(broker):
    worker = dramatiq.Worker(broker, worker_threads=THREAD_COUNT)
    worker.start()
    try:
        yield worker
    finally:
        worker.stop()


@pytest.fixture
def actors(session_actors, worker):
    # Helper to get a reference to the session-scoped actors while
    # cleaning the call history at end of each test.
    yield session_actors
    session_actors.reset()


def test_actors_basic(actors):
    """Basic actors can be invoked successfully."""

    # No actors were called yet.
    assert not actors.actor_calls

    # Let's send some stuff:
    actors.basic.send(123)
    actors.basic.send(234)
    actors.basic.send(456)

    assert_soon(lambda: len(actors.actor_calls) == 3)
    assert actors.actor_calls == ["basic", "basic", "basic"]


def test_actors_delay(actors, broker, db):
    """Delayed actors can be invoked successfully."""

    def get_messages():
        # helper to get all DB messages while ensuring no sleeps or cached data.
        db.expire_all()
        return db.query(DramatiqMessage).all()

    # No actors were called yet.
    assert not actors.actor_calls

    # Send something 'soon'
    enqueue_time = time.monotonic()
    actors.blocking_fn.send_with_options(delay=500)

    # There should immediately be a message enqueued, but it goes to delayed queue
    initial_messages = get_messages()
    assert len(initial_messages) == 1
    assert initial_messages[0].queue == "default.DQ"

    # But after the delay has elapsed, it should be shifted over to the non-delayed queue
    assert_soon(lambda: get_messages()[0].queue == "default")

    # And the actor should be called
    assert_soon(lambda: actors.actor_calls == ["blocking_fn start"])

    # Then let the call complete as normal
    actors.blocking_sem.release()

    # It should eventually dequeue message as normal
    assert_soon(lambda: get_messages() == [])

    # And the call should have completed
    assert actors.actor_calls == ["blocking_fn start", "blocking_fn end"]

    # The time of the call should have been >= our requested delay.
    assert actors.actor_call_times[0] - enqueue_time >= 0.5


def test_actors_retry(actors):
    """Default retry middleware works as usual."""

    actors.succeeds_on_retry.send()

    # Fails, then succeeds on second try, for a total of two calls
    assert_soon(lambda: len(actors.actor_calls) == 2)
    assert actors.actor_calls == ["succeeds_on_retry", "succeeds_on_retry"]


def test_actors_fail(actors, caplog):
    """A failing actor generates a log."""

    actors.always_fails.send()

    # It's allowed to be retried once, for a total of two calls, but ultimately
    # it will fail.
    assert_soon(lambda: len(actors.actor_calls) == 2)
    assert actors.actor_calls == ["always_fails", "always_fails"]

    # Somewhere there should have been a log produced about the failure.
    def failure_message():
        return [m for m in caplog.messages if "message failed" in m]

    assert_soon(failure_message)

    assert len(failure_message()) == 1

    # That message should have all the info, including traceback
    assert "I always fail" in failure_message()[0]


def test_actors_fail_task(db: Session, actors):
    """A failing actor marks corresponding Task as failed."""

    msg = actors.always_fails_blocking.send()

    # The actor is going to fail, but it's not allowed to proceed until we
    # unblock the semaphore. Meanwhile let's set up a Task for that message,
    # similarly how Commit works for real.
    publish = Publish(
        env="test",
        state="COMMITTING",
        id="8b844e25-5664-4c31-9a9e-2720182b5db3",
    )
    task = CommitTask(
        id=msg.message_id,
        publish_id="8b844e25-5664-4c31-9a9e-2720182b5db3",
        state="IN_PROGRESS",
    )
    db.add(publish)
    db.add(task)
    db.commit()

    def task_refreshed_state() -> str:
        db.refresh(task)
        return task.state

    # Allow the actor to proceed now.
    # Semaphore released twice since the actor is set up to retry once.
    actors.blocking_sem.release(2)

    # As a side-effect of the actor failing, the task's state should
    # transition to FAILED.
    assert_soon(lambda: task_refreshed_state() == "FAILED")


def test_mixed_queues(actors, db):
    """Invoking actors from different queues works as expected."""

    actors.basic.send(1)
    actors.basic_other_queue.send(2)

    assert_soon(lambda: len(actors.actor_calls) == 2)
    assert sorted(actors.actor_calls) == ["basic", "basic_other_queue"]


def test_logs_prefixed(actors, caplog):
    """Log messages generated within an actor are prefixed."""

    actors.log_warning.send(task_id="task-abc123", value="some value")

    # Ensure actor is invoked
    assert_soon(lambda: len(actors.actor_calls) == 1)
    assert sorted(actors.actor_calls) == ["log_warning"]

    # When the warning was logged, it should have automatically embedded
    # both the actor name and the task id
    assert (
        "[log_warning task-abc123] warning from actor: some value"
        in caplog.text
    )


def test_logs_request_id(actors, caplog):
    """Log messages include "request_id", propagated from asgi_correlation_id."""

    token = correlation_id.set("aabbccdd")
    try:
        actors.log_warning.send(task_id="task-abc123", value="some value")

        # Ensure actor is invoked
        assert_soon(lambda: len(actors.actor_calls) == 1)
        assert sorted(actors.actor_calls) == ["log_warning"]

        # The logged message should have included the correlation id
        assert '"request_id": "aabbccdd"' in caplog.text
    finally:
        correlation_id.reset(token)


def test_logs_prefixed_threaded(actors, caplog):
    """Log messages generated within an actor-spawned thread are prefixed
    (as long as contextvars.Context was propagated).
    """

    actors.log_warning_from_thread.send(
        task_id="task-abc123", value="some value"
    )

    # Ensure actor is invoked
    assert_soon(lambda: len(actors.actor_calls) == 1)
    assert sorted(actors.actor_calls) == ["log_warning_from_thread"]

    # When the warning was logged, it should have automatically embedded
    # both the actor name and the task id
    assert (
        "[log_warning_from_thread task-abc123] warning from actor: some value"
        in caplog.text
    )


def test_queue_backlog(actors, db):
    """Consumers only prefetch a limited number of messages."""

    # How many messages we expect the system to prefetch.
    prefetch_count = THREAD_COUNT * 2

    # How many messages we'll create.
    message_count = prefetch_count * 2

    # Enqueue many calls to blocking function.  None of them can proceed
    # until we do actors.blocking_sem.release().
    for _ in range(0, message_count):
        actors.blocking_fn.send()

    # We should have created this many messages
    assert db.query(DramatiqMessage).count() == message_count

    # But only this many can be consumed for now...
    assert_soon(
        lambda: db.query(DramatiqMessage)
        .filter(DramatiqMessage.consumer_id != None)
        .count()
        == prefetch_count
    )

    # Let's unblock one of them
    actors.blocking_sem.release()

    # We should now see total message count decrease since a message was allowed
    # to complete successfully
    assert_soon(lambda: db.query(DramatiqMessage).count() == message_count - 1)

    # But the count here should become the same
    assert_soon(
        lambda: db.query(DramatiqMessage)
        .filter(DramatiqMessage.consumer_id != None)
        .count()
        == prefetch_count
    )

    # Let's unblock all the rest
    for _ in range(0, message_count - 1):
        actors.blocking_sem.release()

    # That should allow all messages to be processed (and hence removed)
    assert_soon(lambda: db.query(DramatiqMessage).count() == 0)

    # And all calls should have completed.
    # Note: blocking_fn logs two messages per call
    assert len(actors.actor_calls) == 2 * message_count
