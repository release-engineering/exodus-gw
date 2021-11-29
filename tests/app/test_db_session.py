import uuid

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

from exodus_gw import deps
from exodus_gw.main import app
from exodus_gw.models import Publish

# A hardcoded UUID so we can find what we've created.
TEST_UUID = uuid.UUID("{12345678-1234-5678-1234-567812345678}")


# A testing endpoint which will create an object and then commit,
# rollback or raise based on params
def make_publish(mode: str = None, db: Session = deps.db):
    p = Publish(id=TEST_UUID, env="test", state="PENDING")
    db.add(p)

    if mode == "rollback":
        db.rollback()
    elif mode == "commit":
        db.commit()
    elif mode == "raise":
        raise HTTPException(500)


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
    assert r.ok

    # Should have committed, even though we didn't explicitly request it
    publishes = db.query(Publish).filter(Publish.id == TEST_UUID)
    assert publishes.count() == 1


def test_db_explicit_commit(db):
    """An explicit commit from within an endpoint works as expected."""

    with TestClient(app) as client:
        r = client.post("/test_db_session/make_publish?mode=commit")

    # Should succeed
    assert r.ok

    # Should have committed, as requested
    publishes = db.query(Publish).filter(Publish.id == TEST_UUID)
    assert publishes.count() == 1


def test_db_rollback(db):
    """An explicit rollback from within an endpoint works as expected."""

    with TestClient(app) as client:
        r = client.post("/test_db_session/make_publish?mode=rollback")

    # Should succeed
    assert r.ok

    # Should not have committed anything since we explicitly rolled back
    publishes = db.query(Publish).filter(Publish.id == TEST_UUID)
    assert publishes.count() == 0


def test_db_rollback_on_raise(db):
    """Implicit rollback occurs if endpoint raises an exception."""

    with TestClient(app) as client:
        r = client.post("/test_db_session/make_publish?mode=raise")

    # Should fail since an exception was raised
    assert not r.ok

    # Should not have committed anything since exception was raised
    publishes = db.query(Publish).filter(Publish.id == TEST_UUID)
    assert publishes.count() == 0
