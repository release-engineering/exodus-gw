import uuid

from fastapi import Depends, HTTPException
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

from exodus_gw.database import SessionLocal
from exodus_gw.main import app
from exodus_gw.models import Publish
from exodus_gw.routers.gateway import get_db

# A hardcoded UUID so we can find what we've created.
TEST_UUID = uuid.UUID("{12345678-1234-5678-1234-567812345678}")


# A testing endpoint which will create an object and then commit,
# rollback or raise based on params
@app.post("/test_db_session/make_publish")
def make_publish(mode: str = None, db: Session = Depends(get_db)):
    p = Publish(id=TEST_UUID)
    db.add(p)

    if mode == "rollback":
        db.rollback()
    elif mode == "commit":
        db.commit()
    elif mode == "raise":
        raise HTTPException(500)


def test_db_implicit_commit():
    """Commit occurs if request succeeds."""

    with TestClient(app) as client:
        r = client.post("/test_db_session/make_publish")

    # Should succeed
    assert r.ok

    # Should have committed, even though we didn't explicitly request it
    db = SessionLocal()
    publishes = db.query(Publish).filter(Publish.id == TEST_UUID)
    assert publishes.count() == 1


def test_db_explicit_commit():
    """An explicit commit from within an endpoint works as expected."""

    with TestClient(app) as client:
        r = client.post("/test_db_session/make_publish?mode=commit")

    # Should succeed
    assert r.ok

    # Should have committed, as requested
    db = SessionLocal()
    publishes = db.query(Publish).filter(Publish.id == TEST_UUID)
    assert publishes.count() == 1


def test_db_rollback():
    """An explicit rollback from within an endpoint works as expected."""

    with TestClient(app) as client:
        r = client.post("/test_db_session/make_publish?mode=rollback")

    # Should succeed
    assert r.ok

    # Should not have committed anything since we explicitly rolled back
    db = SessionLocal()
    publishes = db.query(Publish).filter(Publish.id == TEST_UUID)
    assert publishes.count() == 0


def test_db_rollback_on_raise():
    """Implicit rollback occurs if endpoint raises an exception."""

    with TestClient(app) as client:
        r = client.post("/test_db_session/make_publish?mode=raise")

    # Should fail since an exception was raised
    assert not r.ok

    # Should not have committed anything since exception was raised
    db = SessionLocal()
    publishes = db.query(Publish).filter(Publish.id == TEST_UUID)
    assert publishes.count() == 0
