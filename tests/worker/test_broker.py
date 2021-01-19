import logging

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

    def execute(self, sql):
        self._pool._record_execute(sql)
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

    def _record_execute(self, sql):
        self.executes.append((self.in_transaction, sql))


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

    (in_transaction, sql) = pool.executes[0]

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
