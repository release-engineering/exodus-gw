import dramatiq
import pytest
from asgi_correlation_id import correlation_id
from fastapi.testclient import TestClient

from exodus_gw.dramatiq.broker import Broker
from exodus_gw.main import app
from exodus_gw.models import DramatiqMessage


@pytest.fixture(params=["7241e3d6", None])
def test_id(request: pytest.FixtureRequest):
    # This fixture yields and sets different values for the
    # current asgi correlation id. Can be used to test cases
    # where the ID is set vs not set.
    token = None

    if request.param:
        token = correlation_id.set(request.param)

    yield request.param

    if token:
        correlation_id.reset(token)


def test_enqueue(test_id, db):
    """Enqueuing a message creates DB record as expected."""

    with TestClient(app):
        broker = Broker()

        @dramatiq.actor(broker=broker, queue_name="some-queue")
        def some_fn(x, y):
            pass

        some_fn.send(1, y="hello")

    # Should have created one message
    assert db.query(DramatiqMessage).count() == 1

    message = db.query(DramatiqMessage).one()

    # It shouldn't be associated with any consumer yet.
    assert message.consumer_id is None

    # It should have recorded the correct actor and queue
    assert message.actor == "some_fn"
    assert message.queue == "some-queue"

    # Message body should have all the expected elements
    # (won't test specific timestamp value)
    assert message.body.pop("message_timestamp")
    assert message.body == {
        "args": [1],
        "kwargs": {"y": "hello", "correlation_id": test_id},
        "options": {},
    }


def test_enqueue_with_session(db):
    """Enqueuing a message when session is set creates a DB record as expected
    (without committing it).
    """

    with TestClient(app):
        broker = Broker()

        # Let the broker share our session.
        broker.set_session(db)

        @dramatiq.actor(broker=broker)
        def some_fn():
            pass

        some_fn.send()

    # There should be one 'new' object since we didn't commit
    new_objects = list(db.new)
    assert len(new_objects) == 1

    # It should be a message
    assert isinstance(new_objects[0], DramatiqMessage)

    # (sanity check that db.new works as this test expects - should be empty
    # after commit)
    db.commit()
    assert not db.new
