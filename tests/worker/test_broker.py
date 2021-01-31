import asyncio
import logging
import threading

import dramatiq
import pytest
from psycopg2.errors import DuplicateSchema

from exodus_gw.worker.broker import ExodusGwBroker, new_broker


# Helpers to monitor the SQL which (would be) executed by our broker
class FakeCursor:
    def __init__(self, pool):
        self._pool = pool

    def __enter__(self):
        return self

    def __exit__(self, type, value, tb):
        pass

    def execute(self, sql, parameters=None):
        self._pool._record_execute(sql, parameters)
        if self._pool.raises:
            raise self._pool.raises.pop(0)


class FakeConnectionPool:
    def __init__(self):
        self.executes = []
        self.raises = []
        self.in_transaction = 0

    def __enter__(self):
        self.in_transaction += 1
        return self

    def __exit__(self, type, value, tb):
        self.in_transaction -= 1

    def cursor(self):
        return FakeCursor(self)

    def _record_execute(self, sql, parameters):
        self.executes.append((self.in_transaction, sql, parameters))


class FakeSession:
    def __init__(self):
        self.pool = FakeConnectionPool()

    def connection(self):
        return self.pool.cursor()


def test_broker_class(monkeypatch):
    """new_broker uses our class if not in test mode"""

    monkeypatch.delenv("EXODUS_GW_STUB_BROKER")

    broker = new_broker()
    assert isinstance(broker, ExodusGwBroker)


def test_broker_initialize_on_consume():
    """Broker will initialize schema on attempt to consume"""

    pool = FakeConnectionPool()
    broker = ExodusGwBroker(url=None, pool=pool)
    broker.consume("some-queue")

    # A side-effect of creating the consumer should be that we've
    # instantiated our schema
    assert len(pool.executes) == 1

    (in_transaction, sql, parameters) = pool.executes[0]

    # it should have been done in a transaction
    assert in_transaction

    # it should have been our schema initialization SQL
    # (just sampling it here, not hardcoding the entire thing)
    assert "CREATE SCHEMA dramatiq;" in sql


def test_broker_already_initialized(caplog):
    """Broker will tolerate & log if schema is already initialized"""

    logging.getLogger("exodus-gw").setLevel(logging.INFO)

    pool = FakeConnectionPool()
    broker = ExodusGwBroker(url=None, pool=pool)

    # Simulate that the schema already exists
    pool.raises.append(DuplicateSchema())

    # It should not crash...
    broker.consume("some-queue")

    # ...and it should also note that the schema was already in place
    assert "dramatiq schema was already in place" in caplog.messages


def test_broker_enqueues_via_session():
    """If a session is set on broker, SQL is executed via that session"""
    broker = ExodusGwBroker(url=None, pool=object())

    session = FakeSession()
    broker.set_session(session)

    @dramatiq.actor(broker=broker)
    def some_fn():
        pass

    # This should work
    some_fn.send()

    # And it should have executed a statement via the session we provided.
    assert len(session.pool.executes) == 1

    (in_transaction, sql, parameters) = session.pool.executes[0]

    # It should have been some enqueue SQL
    assert 'INSERT INTO "dramatiq"."queue"' in sql


def test_broker_cannot_enqueue_missing_session():
    """Meaningful error is raised if broker is used with no session"""

    broker = ExodusGwBroker(url=None, pool=object())

    broker.set_session(None)

    @dramatiq.actor(broker=broker)
    def some_fn():
        pass

    # We won't be able to send this.
    with pytest.raises(RuntimeError) as exc_info:
        some_fn.send()

    assert (
        "BUG: attempted to use session-aware broker while no session is active"
        in str(exc_info.value)
    )


def test_broker_sessionless_pool_shared_between_threads():
    """When no session is set, the broker's pool is allowed to be shared between threads."""

    # Create a broker with some arbitrary pool.
    pool = object()
    broker = ExodusGwBroker(url=None, pool=pool)

    # Create a helper to check current value of pool and store it somewhere
    # we can see.
    spied_pool = []

    def spy_pool():
        spied_pool.append(broker.pool)

    # Let's check which pool object we get from a new thread.
    thread = threading.Thread(target=spy_pool, name="spy-pool")
    thread.start()
    thread.join(timeout=1.0)
    assert not thread.is_alive()

    # The thread should have seen exactly the same object; i.e. because we haven't
    # set a session on the broker, the pool is not context-aware.
    assert spied_pool[0] is pool


@pytest.mark.asyncio
async def test_broker_context_aware_session():
    """Coroutines running concurrently can set broker session without interfering
    with each other.
    """

    broker = ExodusGwBroker(url=None, pool=object())

    observed1 = []
    observed2 = []
    sequence = []

    async def pool_spy(out):
        # Set a session from this coro (which should create a new pool)
        broker.set_session(FakeSession())

        # Record current pool
        out.append(broker.pool)
        sequence.append(out)

        # Wait a bit, yielding to another coro which will set
        # a different session
        await asyncio.sleep(0.2)

        # Check again:
        out.append(broker.pool)
        sequence.append(out)

    awt1 = pool_spy(observed1)
    awt2 = pool_spy(observed2)

    # Run these concurrently
    await asyncio.gather(awt1, awt2)

    # Execution of the coros should have been interleaved...
    assert sequence[0] != sequence[1]

    # ...yet each coro should not see changes to each other's pool,
    # i.e. each coro saw only one value for pool, and each coro saw
    # a different value than the other
    assert observed1[0] is observed1[1]
    assert observed2[0] is observed2[1]
    assert observed1[0] is not observed2[0]
