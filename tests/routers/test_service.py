import mock
from fastapi.testclient import TestClient

from exodus_gw import models
from exodus_gw.main import app
from exodus_gw.routers import service


def test_healthcheck():
    assert service.healthcheck() == {"detail": "exodus-gw is running"}


def test_healthcheck_worker(stub_worker):
    # This test exercises "real" dramatiq message handling via stub
    # broker & worker, demonstrating that messages can be used from
    # within tests.
    assert service.healthcheck_worker(db=mock.Mock()) == {
        "detail": "background worker is running: ping => pong"
    }


def test_whoami():
    # All work is done by fastapi deserialization, so this doesn't actually
    # do anything except return the passed object.
    context = object()
    assert service.whoami(context=context) is context


def test_get_task(db):
    """The endpoint is able to retrieve task objects stored in DB."""

    publish_id = "48c67d99-5dd6-4939-ad1c-072639eee35a"
    task_id = "8d8a4692-c89b-4b57-840f-b3f0166148d2"

    task = models.Task(
        id=task_id,
        publish_id=publish_id,
        state="NOT_STARTED",
    )

    # Use TestClient to set up the test DB.
    with TestClient(app) as client:
        # Add a task object to the DB.
        db.add(task)
        db.commit()

        # Update the task's state.
        db.refresh(task)
        task.state = "COMPLETE"
        db.commit()

        # Try to look up an invalid ID.
        resp = client.get("/task/%s" % publish_id)

        assert resp.status_code == 404
        assert "No task found" in str(resp.content)

        # Try to look up a valid ID.
        resp = client.get("/task/%s" % task_id)

    # Last request should have succeeded and returned the correct object.
    assert resp.ok
    assert resp.json() == {
        "id": "8d8a4692-c89b-4b57-840f-b3f0166148d2",
        "state": "COMPLETE",
        "publish_id": "48c67d99-5dd6-4939-ad1c-072639eee35a",
        "links": {"self": "/task/8d8a4692-c89b-4b57-840f-b3f0166148d2"},
    }
