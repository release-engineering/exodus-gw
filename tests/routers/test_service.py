from datetime import datetime, timezone

from fastapi.testclient import TestClient

from exodus_gw import models
from exodus_gw.main import app
from exodus_gw.models import DramatiqConsumer
from exodus_gw.routers import service


async def test_healthcheck():
    assert (await service.healthcheck()) == {"detail": "exodus-gw is running"}


def test_healthcheck_worker_healthy(db):
    with TestClient(app) as client:
        # Ensure there's some live consumer.
        db.add(
            DramatiqConsumer(
                id="some-consumer", last_alive=datetime.now(timezone.utc)
            )
        )
        db.commit()

        r = client.get("/healthcheck-worker")

        # It should succeed
        assert r.status_code == 200

        # Should give a generic message
        assert r.json() == {"detail": "background worker is running"}


def test_healthcheck_worker_unhealthy(db):
    with TestClient(app) as client:
        # The only consumer we have is stale.
        db.add(
            DramatiqConsumer(
                id="some-consumer",
                last_alive=datetime(1999, 1, 1, tzinfo=timezone.utc),
            )
        )
        db.commit()

        r = client.get("/healthcheck-worker")

        # It should fail
        assert r.status_code == 500

        # Should give a generic message
        assert r.json() == {"detail": "background workers unavailable"}

        # Code 500 responses should provide a request ID
        assert len(r.headers["X-Request-ID"]) == 8


async def test_whoami():
    # All work is done by fastapi deserialization, so this doesn't actually
    # do anything except return the passed object.
    context = object()
    assert (await service.whoami(context=context)) is context


def test_get_task(db):
    """The endpoint is able to retrieve task objects stored in DB."""

    publish_id = "48c67d99-5dd6-4939-ad1c-072639eee35a"
    task_id = "8d8a4692-c89b-4b57-840f-b3f0166148d2"

    task = models.CommitTask(
        id=task_id,
        publish_id=publish_id,
        state="NOT_STARTED",
    )

    with TestClient(app) as client:
        # Add a task object to the DB.
        db.add(task)
        db.commit()

        # Try to look up an invalid ID.
        resp = client.get("/task/%s" % publish_id)

        assert resp.status_code == 404
        assert "No task found" in str(resp.content)

        # Try to look up a valid ID.
        resp = client.get("/task/%s" % task_id)

    # Last request should have succeeded and returned the correct object.
    assert resp.status_code == 200
    assert resp.json()["publish_id"] == publish_id


def test_redirect_to_docs():
    """Accessing / from a browser redirects to docs."""

    with TestClient(app) as client:
        resp = client.get(
            "/",
            headers={
                "Accept": "text/html,application/xhtml+xml,application/xml"
            },
        )

        # Note the TestClient follows redirects, so this is actually
        # covering both the fact that a redirect happened and that
        # the target URL serves up redoc stuff.
        assert resp.status_code == 200
        assert '<redoc spec-url="/openapi.json">' in resp.text


def test_root_non_browser():
    """Accessing / from non-browser gives 404."""

    with TestClient(app) as client:
        resp = client.get("/")
        assert resp.status_code == 404
