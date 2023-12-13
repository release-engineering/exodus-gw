import logging
import threading
from unittest import mock

import dramatiq
from sqlalchemy import text

from exodus_gw.dramatiq.middleware import PostgresNotifyMiddleware


class FakeSelect:
    def __init__(self):
        self.event = threading.Event()
        self.exception = None

    def __call__(self, rlist, wlist, xlist, timeout):
        if self.exception:
            raise self.exception
        if self.event.wait(timeout):
            self.event.clear()
            return (rlist, wlist, xlist)
        return ([], [], [])


class FakeBroker(dramatiq.Broker):
    def __init__(self):
        super().__init__(middleware=[])

        self.notifies = threading.Semaphore(0)
        self.session = None

    def notify(self):
        self.notifies.release()


def test_listen_thread(caplog):
    """Listen thread calls notify on broker at appropriate times."""

    caplog.set_level(logging.INFO)
    logging.getLogger("exodus-gw").setLevel(logging.INFO)

    db_engine = mock.MagicMock()
    db_engine.url = "postgresql://whatever"
    select = FakeSelect()
    broker = FakeBroker()
    mw = PostgresNotifyMiddleware(lambda: db_engine, 0.1)
    broker.add_middleware(mw)

    with mock.patch("select.select", new=select):
        # thread starts here
        broker.emit_before("worker_boot", object())

        # If we allow select to return True...
        select.event.set()

        # Then we should receive a notify soon
        assert broker.notifies.acquire(timeout=2.0)

        # But just one
        assert not broker.notifies.acquire(timeout=0.2)

        # We should get another one whenever select doesn't time out
        select.event.set()
        assert broker.notifies.acquire(timeout=2.0)

        # And it should recover automatically if the thread breaks
        select.exception = RuntimeError("something wrong")

        # (it needs one more iteration before it will crash)
        select.event.set()
        assert broker.notifies.acquire(timeout=2.0)

        # Now it has crashed, let it recover
        select.exception = None

        # And it should continue to work afterward
        for _ in range(0, 5):
            select.event.set()
            assert broker.notifies.acquire(timeout=2.0)

        # Meanwhile the exception should have been logged
        assert "something wrong" in caplog.text

        # This shuts down the listener
        broker.emit_before("worker_shutdown", object())


def test_notifies():
    """Middleware executes postgres NOTIFY statements when relevant events occur."""

    db_engine = mock.MagicMock()
    db_engine.url = "postgresql://whatever"
    db_conn = db_engine.connect().__enter__()
    broker = FakeBroker()
    mw = PostgresNotifyMiddleware(lambda: db_engine)
    broker.add_middleware(mw)

    # These should all result in notifies occurring
    broker.emit_after("ack", None)
    broker.emit_after("nack", None)
    broker.emit_after("enqueue", None, None)

    assert [call.args[0].text for call in db_conn.execute.mock_calls] == 3 * [
        "NOTIFY dramatiq"
    ]

    # And if the broker has a session, it should use that
    broker.session = mock.MagicMock()
    broker.emit_after("ack", None)

    assert [
        call.args[0].text for call in broker.session.execute.mock_calls
    ] == ["NOTIFY dramatiq"]
