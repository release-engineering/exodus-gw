import uuid
from typing import Optional

import pytest
from fastapi import HTTPException, Request
from fastapi.testclient import TestClient
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import Session

from exodus_gw import deps
from exodus_gw.main import app
from exodus_gw.models import Publish

# A hardcoded UUID so we can find what we've created.
TEST_UUID = "12345678-1234-5678-1234-567812345678"


# A testing endpoint which will create an object and then commit,
# rollback or raise based on params
def make_publish(
    request: Request, mode: Optional[str] = None, db: Session = deps.db
):
    p = Publish(id=TEST_UUID, env="test", state="PENDING")
    db.add(p)

    if mode == "rollback":
        db.rollback()
    elif mode == "commit":
        db.commit()
    elif mode == "raise":
        raise HTTPException(500)
    elif mode == "raise-db":
        raise DBAPIError("err", "params", BaseException())
    elif mode == "raise-db-and-resolve":
        request.state.make_publish_count = (
            getattr(request.state, "make_publish_count", 0) + 1
        )
        if request.state.make_publish_count == 1:
            raise DBAPIError("err", "params", BaseException())
        db.commit()


@pytest.fixture(scope="module", autouse=True)
def add_route():
    # This fixture registers make_publish as a route on app prior to test,
    # then unregisters it after all tests in this module have completed.
    #
    # This is needed because we're reusing the shared 'app' object, and we
    # should not allow this test's custom route to leak into other tests
    # where it could impact the result.
    app.post("/test_db_session/make_publish")(make_publish)
    added_route = app.routes[-1]

    yield

    app.routes.remove(added_route)


def test_db_implicit_commit(db):
    """Commit occurs if request succeeds."""

    with TestClient(app) as client:
        r = client.post("/test_db_session/make_publish")

    # Should succeed
    assert r.status_code == 200

    # Should have committed, even though we didn't explicitly request it
    publishes = db.query(Publish).filter(Publish.id == TEST_UUID)
    assert publishes.count() == 1


def test_db_explicit_commit(db):
    """An explicit commit from within an endpoint works as expected."""

    with TestClient(app) as client:
        r = client.post("/test_db_session/make_publish?mode=commit")

    # Should succeed
    assert r.status_code == 200

    # Should have committed, as requested
    publishes = db.query(Publish).filter(Publish.id == TEST_UUID)
    assert publishes.count() == 1


def test_db_rollback(db):
    """An explicit rollback from within an endpoint works as expected."""

    with TestClient(app) as client:
        r = client.post("/test_db_session/make_publish?mode=rollback")

    # Should succeed
    assert r.status_code == 200

    # Should not have committed anything since we explicitly rolled back
    publishes = db.query(Publish).filter(Publish.id == TEST_UUID)
    assert publishes.count() == 0


def test_db_rollback_on_raise(db):
    """Implicit rollback occurs if endpoint raises an exception."""

    with TestClient(app) as client:
        r = client.post("/test_db_session/make_publish?mode=raise")

    # Should fail since an exception was raised
    assert r.status_code == 500

    # Should not have committed anything since exception was raised
    publishes = db.query(Publish).filter(Publish.id == TEST_UUID)
    assert publishes.count() == 0


def test_db_rollback_on_raise_db(db):
    """A deliberate rollback occurs if an endpoint raises a DBAPIError exception.
    If the DBAPIError exception is not resolved within the defined number of tries,
    the DBAPIError exception will be raised, and the endpoint will fail."""

    # The request will eventually raise the DBAPI error after exceeding the
    # configured number of retries.
    with pytest.raises(DBAPIError):
        with TestClient(app) as client:
            r = client.post("/test_db_session/make_publish?mode=raise-db")

        # Should fail since an exception was raised
        assert r.status_code == 500

        # Should not have committed anything since exception was raised
        publishes = db.query(Publish).filter(Publish.id == TEST_UUID)
        assert publishes.count() == 0


def test_db_raise_error_and_resolve(db):
    """If an endpoint raises a DBAPIError exception, the request is retried. If
    the exception is resolved within the defined number of tries, the endpoint
    should work as expected."""

    with TestClient(app) as client:
        r = client.post(
            "/test_db_session/make_publish?mode=raise-db-and-resolve"
        )

    # Should not fail since exception was retried and resolved
    assert r.status_code == 200

    # Should have committed something since exception was retried and resolved
    publishes = db.query(Publish).filter(Publish.id == TEST_UUID)
    assert publishes.count() == 1
