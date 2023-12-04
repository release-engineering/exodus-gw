import json
from datetime import datetime, timedelta

import mock
import pytest
import sqlalchemy.orm
from fastapi import HTTPException
from fastapi.testclient import TestClient
from freezegun import freeze_time

from exodus_gw import routers, schemas
from exodus_gw.main import app
from exodus_gw.models import CommitTask, Item, Publish, Task
from exodus_gw.settings import Environment, Settings, get_environment


@pytest.mark.parametrize(
    "env",
    [
        "test",
        "test2",
        "test3",
    ],
)
def test_publish_env_exists(env, db, auth_header):
    with TestClient(app) as client:
        r = client.post(
            "/%s/publish" % env,
            headers=auth_header(roles=["%s-publisher" % env]),
        )

    # Should succeed
    assert r.status_code == 200

    # Should have returned a publish object
    publish_id = r.json()["id"]

    publishes = db.query(Publish).filter(Publish.id == publish_id)
    assert publishes.count() == 1


def test_publish_env_doesnt_exist(auth_header):
    with TestClient(app) as client:
        r = client.post(
            "/foo/publish", headers=auth_header(roles=["foo-publisher"])
        )

    # It should fail
    assert r.status_code == 404

    # It should mention that it was a bad environment
    assert r.json() == {"detail": "Invalid environment='foo'"}


def test_publish_links(mock_db_session):
    publish = routers.publish.publish(
        env=Environment(
            "test",
            "some-profile",
            "some-bucket",
            "some-table",
            "some-config-table",
            "some/test/url",
            "a12c3b4fe56",
        ),
        db=mock_db_session,
    )

    # The schema (realistic result) of the publish
    # should contain accurate links.
    assert schemas.Publish(**publish.__dict__).links == {
        "self": "/test/publish/%s" % publish.id,
        "commit": "/test/publish/%s/commit" % publish.id,
    }


def test_update_publish_items_typical(db, auth_header):
    """PUTting some items on a publish creates expected objects in DB."""

    publish_id = "11224567-e89b-12d3-a456-426614174000"

    publish = Publish(id=publish_id, env="test", state="PENDING")

    with TestClient(app) as client:
        # Ensure a publish object exists
        db.add(publish)
        db.commit()

        # Try to add some items to it
        r = client.put(
            "/test/publish/%s" % publish_id,
            json=[
                {
                    "web_uri": "/uri1",
                    "object_key": "1" * 64,
                    "content_type": "application/octet-stream",
                },
                {
                    "web_uri": "/uri2",
                    "object_key": "2" * 64,
                    "content_type": "application/octet-stream",
                },
                {
                    "web_uri": "/uri3",
                    "link_to": "/uri1",
                },
                {
                    "web_uri": "/uri4",
                    "object_key": "absent",
                },
            ],
            headers=auth_header(roles=["test-publisher"]),
        )

    # It should have succeeded
    assert r.status_code == 200

    # Publish object should now have matching items
    db.refresh(publish)

    items = sorted(publish.items, key=lambda item: item.web_uri)
    item_dicts = [
        {
            "web_uri": item.web_uri,
            "object_key": item.object_key,
            "content_type": item.content_type,
            "link_to": item.link_to,
        }
        for item in items
    ]

    # Should have stored exactly what we asked for
    assert item_dicts == [
        {
            "web_uri": "/uri1",
            "object_key": "1" * 64,
            "content_type": "application/octet-stream",
            "link_to": "",
        },
        {
            "web_uri": "/uri2",
            "object_key": "2" * 64,
            "content_type": "application/octet-stream",
            "link_to": "",
        },
        {
            "web_uri": "/uri3",
            "object_key": "",
            "content_type": "",
            "link_to": "/uri1",
        },
        {
            "web_uri": "/uri4",
            "object_key": "absent",
            "content_type": "",
            "link_to": "",
        },
    ]


def test_update_publish_items_path_normalization(db, auth_header):
    """URI and link target paths are normalized in PUT items."""

    publish_id = "11224567-e89b-12d3-a456-426614174000"

    publish = Publish(id=publish_id, env="test", state="PENDING")

    with TestClient(app) as client:
        # Ensure a publish object exists
        db.add(publish)
        db.commit()

        # Add an item to it with some messy paths
        r = client.put(
            "/test/publish/%s" % publish_id,
            json=[
                {"web_uri": "some/path", "object_key": "1" * 64},
                {"web_uri": "link/to/some/path", "link_to": "/some/path"},
            ],
            headers=auth_header(roles=["test-publisher"]),
        )

    # It should have succeeded
    assert r.status_code == 200

    # Publish object should now have matching items
    db.refresh(publish)

    items = sorted(publish.items, key=lambda item: item.link_to)
    item_dicts = [
        {
            "web_uri": item.web_uri,
            "object_key": item.object_key,
            "link_to": item.link_to,
        }
        for item in items
    ]

    # Should have stored normalized web_uri and link_to paths
    assert item_dicts == [
        {"web_uri": "/some/path", "object_key": "1" * 64, "link_to": ""},
        {
            "web_uri": "/link/to/some/path",
            "object_key": "",
            "link_to": "/some/path",
        },
    ]


def test_update_publish_items_invalid_publish(db, auth_header):
    """PUTting items on a completed publish fails with code 409."""

    publish_id = "11224567-e89b-12d3-a456-426614174000"

    publish = Publish(id=publish_id, env="test", state="COMPLETE")

    with TestClient(app) as client:
        # ensure a publish object exists
        db.add(publish)
        db.commit()

        # Try to add some items to it
        r = client.put(
            "/test/publish/%s" % publish_id,
            json=[
                {
                    "web_uri": "/uri1",
                    "object_key": "1" * 64,
                },
            ],
            headers=auth_header(roles=["test-publisher"]),
        )

    # It should have failed with 409
    assert r.status_code == 409
    assert r.json() == {
        "detail": "Publish %s in unexpected state, 'COMPLETE'" % publish_id
    }


def test_update_publish_items_no_uri(db, auth_header):
    """PUTting an item with no web_uri fails validation."""

    publish_id = "11224567-e89b-12d3-a456-426614174000"

    publish = Publish(id=publish_id, env="test", state="PENDING")

    with TestClient(app) as client:
        # ensure a publish object exists
        db.add(publish)
        db.commit()

        # Try to add an item to it
        r = client.put(
            "/test/publish/%s" % publish_id,
            json=[
                {
                    "web_uri": "",
                    "link_to": "/uri1",
                },
            ],
            headers=auth_header(roles=["test-publisher"]),
        )

    expected_item = {
        "web_uri": "",
        "object_key": "",
        "content_type": "",
        "link_to": "/uri1",
    }

    # It should have failed with 400
    assert r.status_code == 400
    assert r.json() == {"detail": ["Value error, No URI: %s" % expected_item]}
    # It should include non-empty request header
    assert len(r.headers["X-Request-ID"]) == 8


@freeze_time("2023-04-26 14:43:13+00:00")
def test_update_publish_items_existing_uri(db, auth_header):
    """PUTting an item which item's web_uri already exists creates expected objects in DB."""

    publish_id = "11224567-e89b-12d3-a456-426614174000"

    new_updated = datetime(2023, 4, 26, 14, 43, 13)
    prev_updated = new_updated - timedelta(hours=2)

    publish = Publish(
        id=publish_id,
        env="test",
        state="PENDING",
        items=[
            Item(
                web_uri="/uri1",
                object_key="1" * 64,
                publish_id=publish_id,
                dirty=False,
                updated=prev_updated,
            ),
            Item(
                web_uri="/uri2",
                object_key="2" * 64,
                publish_id=publish_id,
                dirty=False,
                updated=prev_updated,
            ),
        ],
    )

    with TestClient(app) as client:
        # Ensure a publish object exists
        db.add(publish)
        db.commit()

        # Try to add an item which item's web_uri already exists
        r = client.put(
            "/test/publish/%s" % publish_id,
            json=[
                {
                    "web_uri": "/uri1",
                    "object_key": "3" * 64,
                },
            ],
            headers=auth_header(roles=["test-publisher"]),
        )

    # It should have succeeded
    assert r.status_code == 200

    # Publish object should now have matching items
    db.refresh(publish)

    items = sorted(publish.items, key=lambda item: item.web_uri)
    item_dicts = [
        {
            "web_uri": item.web_uri,
            "object_key": item.object_key,
            "dirty": item.dirty,
            "updated": item.updated,
        }
        for item in items
    ]

    assert item_dicts == [
        {
            # /uri1 was updated, so the object_key/dirty/updated
            # are all different from before.
            "web_uri": "/uri1",
            "object_key": "3" * 64,
            "dirty": True,
            "updated": new_updated,
        },
        {
            # /uri2 was not updated, so it's not dirty and still
            # has the old update time.
            "web_uri": "/uri2",
            "object_key": "2" * 64,
            "dirty": False,
            "updated": prev_updated,
        },
    ]


def test_update_publish_items_invalid_item(db, auth_header):
    """PUTting an item without object_key or link_to fails validation."""

    publish_id = "11224567-e89b-12d3-a456-426614174000"

    publish = Publish(id=publish_id, env="test", state="PENDING")

    with TestClient(app) as client:
        # ensure a publish object exists
        db.add(publish)
        db.commit()

        # Try to add an item to it
        r = client.put(
            "/test/publish/%s" % publish_id,
            json=[{"web_uri": "/uri1"}],
            headers=auth_header(roles=["test-publisher"]),
        )

    expected_item = {
        "web_uri": "/uri1",
        "object_key": "",
        "content_type": "",
        "link_to": "",
    }

    # It should have failed with 400
    assert r.status_code == 400
    assert r.json() == {
        "detail": [
            "Value error, No object key or link target: %s" % expected_item
        ]
    }
    # It should include non-empty request header
    assert len(r.headers["X-Request-ID"]) == 8


def test_update_publish_items_rejects_autoindex(db, auth_header):
    """PUTting an item explicitly using the autoindex filename fails validation
    when the object key is not 'absent'.
    """

    publish_id = "11224567-e89b-12d3-a456-426614174000"

    publish = Publish(id=publish_id, env="test", state="PENDING")

    with TestClient(app) as client:
        # ensure a publish object exists
        db.add(publish)
        db.commit()

        # Try to add an item to it
        r = client.put(
            "/test/publish/%s" % publish_id,
            json=[
                {
                    "web_uri": "/foo/bar/.__exodus_autoindex",
                    "object_key": "1" * 64,
                }
            ],
            headers=auth_header(roles=["test-publisher"]),
        )

    # It should have failed with 400
    assert r.status_code == 400

    # It should tell the reason why
    assert r.json() == {
        "detail": [
            "Value error, Invalid URI /foo/bar/.__exodus_autoindex: filename is reserved"
        ]
    }
    # It should include non-empty request header
    assert len(r.headers["X-Request-ID"]) == 8


def test_update_publish_items_accepts_absent_autoindex(db, auth_header):
    """PUTting an item explicitly using the autoindex filename is accepted if
    the object key is 'absent'.
    """

    publish_id = "11224567-e89b-12d3-a456-426614174000"

    publish = Publish(id=publish_id, env="test", state="PENDING")

    with TestClient(app) as client:
        # ensure a publish object exists
        db.add(publish)
        db.commit()

        # Try to add an item to it
        r = client.put(
            "/test/publish/%s" % publish_id,
            json=[
                {
                    "web_uri": "/foo/bar/.__exodus_autoindex",
                    "object_key": "absent",
                }
            ],
            headers=auth_header(roles=["test-publisher"]),
        )

    # It should have succeeded
    assert r.status_code == 200

    # And should have added the item normally
    db.refresh(publish)
    assert [(i.web_uri, i.object_key) for i in publish.items] == [
        ("/foo/bar/.__exodus_autoindex", "absent")
    ]


def test_update_publish_items_link_and_key(db, auth_header):
    """PUTting an item with both link_to and object_key fails validation."""

    publish_id = "11224567-e89b-12d3-a456-426614174000"

    publish = Publish(id=publish_id, env="test", state="PENDING")

    with TestClient(app) as client:
        # ensure a publish object exists
        db.add(publish)
        db.commit()

        # Try to add an item to it
        r = client.put(
            "/test/publish/%s" % publish_id,
            json=[
                {
                    "web_uri": "/uri2",
                    "object_key": "1" * 64,
                    "link_to": "/uri1",
                },
            ],
            headers=auth_header(roles=["test-publisher"]),
        )

    expected_item = {
        "web_uri": "/uri2",
        "object_key": "1" * 64,
        "content_type": "",
        "link_to": "/uri1",
    }

    # It should have failed with 400
    assert r.status_code == 400
    assert r.json() == {
        "detail": [
            "Value error, Both link target and object key present: %s"
            % expected_item
        ]
    }
    # It should include non-empty request header
    assert len(r.headers["X-Request-ID"]) == 8


def test_update_publish_items_link_content_type(db, auth_header):
    """PUTting an item with link_to and content_type fails validation."""

    publish_id = "11224567-e89b-12d3-a456-426614174000"

    publish = Publish(id=publish_id, env="test", state="PENDING")

    with TestClient(app) as client:
        # ensure a publish object exists
        db.add(publish)
        db.commit()

        # Try to add an item to it
        r = client.put(
            "/test/publish/%s" % publish_id,
            json=[
                {
                    "web_uri": "/uri2",
                    "link_to": "/uri1",
                    "content_type": "application/octet-stream",
                },
            ],
            headers=auth_header(roles=["test-publisher"]),
        )

    expected_item = {
        "web_uri": "/uri2",
        "object_key": "",
        "content_type": "application/octet-stream",
        "link_to": "/uri1",
    }
    # It should have failed with 400
    assert r.status_code == 400
    assert r.json() == {
        "detail": [
            "Value error, Content type specified for link: %s" % expected_item
        ]
    }
    # It should include non-empty request header
    assert len(r.headers["X-Request-ID"]) == 8


def test_update_publish_items_invalid_object_key(db, auth_header):
    """PUTting an item with an non-sha256sum object_key fails validation."""

    publish_id = "11224567-e89b-12d3-a456-426614174000"

    publish = Publish(id=publish_id, env="test", state="PENDING")

    with TestClient(app) as client:
        # ensure a publish object exists
        db.add(publish)
        db.commit()

        # Try to add an item to it
        r = client.put(
            "/test/publish/%s" % publish_id,
            json=[
                {
                    "web_uri": "/uri2",
                    "object_key": "somethingshyof64_with!non-alphanum$",
                },
            ],
            headers=auth_header(roles=["test-publisher"]),
        )

    expected_item = {
        "web_uri": "/uri2",
        "object_key": "somethingshyof64_with!non-alphanum$",
        "content_type": "",
        "link_to": "",
    }

    # It should have failed with 400
    assert r.status_code == 400
    assert r.json() == {
        "detail": [
            "Value error, Invalid object key; must be sha256sum: %s"
            % expected_item
        ]
    }
    # It should include non-empty request header
    assert len(r.headers["X-Request-ID"]) == 8


def test_update_publish_absent_items_with_content_type(db, auth_header):
    """PUTting an absent item with a content type fails validation."""

    publish_id = "11224567-e89b-12d3-a456-426614174000"

    publish = Publish(id=publish_id, env="test", state="PENDING")

    with TestClient(app) as client:
        # ensure a publish object exists
        db.add(publish)
        db.commit()

        # Try to add an item to it
        r = client.put(
            "/test/publish/%s" % publish_id,
            json=[
                {
                    "web_uri": "/uri1",
                    "object_key": "absent",
                    "content_type": "application/octet-stream",
                },
            ],
            headers=auth_header(roles=["test-publisher"]),
        )

    expected_item = {
        "web_uri": "/uri1",
        "object_key": "absent",
        "content_type": "application/octet-stream",
        "link_to": "",
    }

    # It should have failed with 400
    assert r.status_code == 400
    assert r.json() == {
        "detail": [
            "Value error, Cannot set content type when object_key is 'absent': %s"
            % expected_item
        ]
    }
    # It should include non-empty request header
    assert len(r.headers["X-Request-ID"]) == 8


def test_update_publish_items_invalid_content_type(db, auth_header):
    """PUTting an item with a non-MIME type content type fails validation."""

    publish_id = "11224567-e89b-12d3-a456-426614174000"

    publish = Publish(id=publish_id, env="test", state="PENDING")

    with TestClient(app) as client:
        # ensure a publish object exists
        db.add(publish)
        db.commit()

        # Try to add an item to it
        r = client.put(
            "/test/publish/%s" % publish_id,
            json=[
                {
                    "web_uri": "/uri2",
                    "object_key": "1" * 64,
                    "content_type": "type_nosubtype",
                },
            ],
            headers=auth_header(roles=["test-publisher"]),
        )

    expected_item = {
        "web_uri": "/uri2",
        "object_key": "1" * 64,
        "content_type": "type_nosubtype",
        "link_to": "",
    }

    # It should have failed with 400
    assert r.status_code == 400
    assert r.json() == {
        "detail": ["Value error, Invalid content type: %s" % expected_item]
    }
    # It should include non-empty request header
    assert len(r.headers["X-Request-ID"]) == 8


def test_update_publish_items_no_publish(auth_header):
    publish_id = "11224567-e89b-12d3-a456-426614174000"
    with TestClient(app) as client:
        # Try to add an item to non-existent publish
        r = client.put(
            "/test/publish/%s" % publish_id,
            json=[
                {
                    "web_uri": "/uri2",
                    "object_key": "1" * 64,
                    "content_type": "text/plain",
                },
            ],
            headers=auth_header(roles=["test-publisher"]),
        )

    assert r.status_code == 404
    assert r.json() == {"detail": "No publish found for ID %s" % publish_id}


@pytest.mark.parametrize(
    "deadline,commit_mode",
    [(None, None), ("2022-07-25T15:47:47Z", None), (None, "phase1")],
    ids=["typical", "with deadline", "phase1"],
)
@freeze_time("2023-04-26 14:43:13.570034+00:00")
def test_commit_publish(deadline, commit_mode, auth_header, db, caplog):
    """Ensure commit_publish delegates to worker and creates task."""

    # server is expected to apply default of phase2 if commit mode was unspecified.
    expected_commit_mode = commit_mode or "phase2"

    publish_id = "11224567-e89b-12d3-a456-426614174000"

    publish = Publish(id=publish_id, env="test", state="PENDING")

    url = "/test/publish/11224567-e89b-12d3-a456-426614174000/commit"

    params = {}
    if deadline:
        params["deadline"] = deadline
    if commit_mode:
        params["commit_mode"] = commit_mode

    with TestClient(app) as client:
        # ensure a publish object exists
        db.add(publish)
        db.commit()

        # Try to commit it
        r = client.post(
            url, params=params, headers=auth_header(roles=["test-publisher"])
        )

    # It should have succeeded
    assert r.status_code == 200

    # It should return an appropriate task object
    json_r = r.json()
    assert json_r["links"]["self"] == "/task/%s" % json_r["id"]
    assert json_r["publish_id"] == "11224567-e89b-12d3-a456-426614174000"
    if deadline:
        # 'Z' suffix is dropped when stored as datetime in the database
        assert json_r["deadline"] == "2022-07-25T15:47:47"

    for message, event in [
        (
            (
                "Access permitted; "
                "path=/test/publish/11224567-e89b-12d3-a456-426614174000/commit, "
                "user=user fake-user, role=test-publisher"
            ),
            "auth",
        ),
        (
            f"Enqueued {expected_commit_mode} commit for '11224567-e89b-12d3-a456-426614174000'",
            "publish",
        ),
    ]:
        assert (
            json.dumps(
                {
                    "level": "INFO",
                    "logger": "exodus-gw",
                    "time": "2023-04-26 14:43:13.570",
                    "request_id": r.headers["X-Request-ID"],
                    "message": message,
                    "event": event,
                    "success": True,
                }
            )
            in caplog.text
        )


def test_commit_publish_phase1(
    auth_header, db: sqlalchemy.orm.Session, caplog
):
    """Ensure distinct behaviors of phase1 commit:

    - can be invoked more than once for a single publish
    - does not cause the publish to change state
    """

    commit_count = 3
    publish_id = "11224567-e89b-12d3-a456-426614174000"

    publish = Publish(id=publish_id, env="test", state="PENDING")

    url = "/test/publish/11224567-e89b-12d3-a456-426614174000/commit"

    task_ids = []
    with TestClient(app) as client:
        db.add(publish)
        db.commit()

        # We should be able to commit this publish *multiple* times
        # since we're requesting a phase1 commit.
        for _ in range(0, commit_count):
            r = client.post(
                url,
                params={"commit_mode": "phase1"},
                headers=auth_header(roles=["test-publisher"]),
            )

            # It should have succeeded
            assert r.status_code == 200

            # Keep task IDs for later
            task_ids.append(r.json()["id"])

    # The publish object should still be PENDING.
    db.refresh(publish)
    assert publish.state == "PENDING"

    # Get all the created tasks
    tasks = db.query(CommitTask).all()

    # It should have made a separate task per commit request
    assert len(tasks) == commit_count

    # Matching the IDs returned from the API
    assert sorted([t.id for t in tasks]) == sorted(task_ids)

    for task in tasks:
        # Should be associated with our publish
        assert task.publish_id == publish.id

        # Should be marked as a phase1 publish
        assert task.commit_mode == "phase1"


def test_commit_publish_bad_deadline(auth_header, db):
    publish_id = "11224567-e89b-12d3-a456-426614174000"

    publish = Publish(id=publish_id, env="test", state="PENDING")

    url = "/test/publish/11224567-e89b-12d3-a456-426614174000/commit"
    url += "?deadline=07/25/2022 3:47:47 PM"

    with TestClient(app) as client:
        # ensure a publish object exists
        db.add(publish)
        db.commit()

        # Try to commit it
        r = client.post(url, headers=auth_header(roles=["test-publisher"]))

    assert r.status_code == 400
    assert r.json()["detail"] == (
        "ValueError(\"time data '07/25/2022 3:47:47 PM' does not match "
        "format '%Y-%m-%dT%H:%M:%SZ'\")"
    )


def test_commit_publish_bad_mode(auth_header, db):
    publish_id = "11224567-e89b-12d3-a456-426614174000"

    publish = Publish(id=publish_id, env="test", state="PENDING")

    url = "/test/publish/11224567-e89b-12d3-a456-426614174000/commit"
    url += "?commit_mode=bad"

    with TestClient(app) as client:
        # ensure a publish object exists
        db.add(publish)
        db.commit()

        # Try to commit it
        r = client.post(url, headers=auth_header(roles=["test-publisher"]))

    # It should tell me the request was invalid
    assert r.status_code == 400

    # It should tell me why
    assert r.json()["detail"] == ["Input should be 'phase1' or 'phase2'"]


@mock.patch("exodus_gw.worker.commit")
def test_commit_publish_in_progress(mock_commit, fake_publish, db):
    """Ensure commit_publish is idempotent."""

    # Simulate that this publish was already committed, assigned to
    # an in-progress task.
    fake_publish.state = schemas.PublishStates.committing
    task = CommitTask(
        id="8d8a4692-c89b-4b57-840f-b3f0166148d2",
        publish_id=fake_publish.id,
        state=schemas.TaskStates.in_progress,
    )
    db.add(fake_publish)
    db.add(task)
    db.commit()

    publish_task = routers.publish.commit_publish(
        env=get_environment("test"),
        publish_id=fake_publish.id,
        db=db,
        settings=Settings(),
        commit_mode=None,
    )

    # It should have returned the task associated with the publish.
    assert isinstance(publish_task, Task)
    assert publish_task.id == task.id
    assert publish_task.publish_id == fake_publish.id

    # It should not have called worker.commit.
    mock_commit.assert_not_called()


@mock.patch("exodus_gw.worker.commit")
def test_commit_publish_prev_completed(mock_commit, fake_publish, db):
    """Ensure commit_publish fails for publishes in invalid state."""

    db.add(fake_publish)
    # Simulate that this publish was published.
    fake_publish.state = schemas.PublishStates.committed
    db.commit()

    with pytest.raises(HTTPException) as exc_info:
        routers.publish.commit_publish(
            env=get_environment("test"),
            publish_id=fake_publish.id,
            db=db,
            settings=Settings(),
            commit_mode=None,
        )

    assert exc_info.value.status_code == 409
    assert (
        exc_info.value.detail
        == "Publish %s in unexpected state, 'COMMITTED'" % fake_publish.id
    )

    mock_commit.assert_not_called()


@mock.patch("exodus_gw.worker.commit")
def test_commit_publish_linked_items(mock_commit, fake_publish, db):
    """Ensure commit_publish correctly resolves links."""

    # Whole items
    item1 = Item(
        web_uri="/the/path",
        object_key="1" * 64,
        publish_id=fake_publish.id,
        link_to=None,  # It should be able to handle None/NULL link_to values...
        content_type="some type",
    )
    item2 = Item(
        web_uri="/another/path",
        object_key="2" * 64,
        publish_id=fake_publish.id,
        link_to="",  # ...and empty string link_to values...
        content_type="another type",
    )
    item3 = Item(
        web_uri="/some/different/path",
        object_key="3" * 64,
        publish_id=fake_publish.id,
    )
    # Linked items
    ln_item1 = Item(
        web_uri="/alternate/route/to/the/path",
        link_to="/the/path",
        publish_id=fake_publish.id,
    )
    ln_item2 = Item(
        web_uri="/alternate/route/to/another/path",
        link_to="/another/path",
        publish_id=fake_publish.id,
    )
    fake_publish.items.extend([item1, item2, item3, ln_item1, ln_item2])

    db.add(fake_publish)
    db.commit()

    publish_task = routers.publish.commit_publish(
        env=get_environment("test"),
        publish_id=fake_publish.id,
        db=db,
        settings=Settings(),
        commit_mode=None,
    )

    # Should've filled ln_item1's object_key with that of item1.
    assert ln_item1.object_key == "1" * 64
    # Should've filled ln_item2's object_key with that of item2.
    assert ln_item2.object_key == "2" * 64

    # Should've filled ln_item1's content_type with that of item1.
    assert ln_item1.content_type == "some type"
    # Should've filled ln_item2's content_type with that of item2.
    assert ln_item2.content_type == "another type"

    # Should've unset the link_to, since links have been resolved.
    assert ln_item1.link_to is None
    assert ln_item2.link_to is None

    # Should've created and sent task.
    assert isinstance(publish_task, Task)

    mock_commit.assert_has_calls(
        calls=[
            mock.call.send(
                publish_id="123e4567-e89b-12d3-a456-426614174000",
                env="test",
                from_date=mock.ANY,
                commit_mode="phase2",
            )
        ],
    )


@mock.patch("exodus_gw.worker.commit")
def test_commit_publish_unresolved_links(mock_commit, fake_publish, db):
    """Ensure commit_publish raises for unresolved links."""

    # Add an item with link to a non-existent item.
    ln_item = Item(
        web_uri="/alternate/route/to/bad/path",
        object_key="",
        link_to="/bad/path",
        publish_id=fake_publish.id,
    )
    fake_publish.items.append(ln_item)

    db.add(fake_publish)
    db.commit()

    with pytest.raises(HTTPException) as exc_info:
        routers.publish.commit_publish(
            env=get_environment("test"),
            publish_id=fake_publish.id,
            db=db,
            settings=Settings(),
            commit_mode=None,
        )

    assert exc_info.value.status_code == 400
    assert (
        exc_info.value.detail
        == "Unable to resolve item object_key:\n\tURI: '%s'\n\tLink: '%s'"
        % (ln_item.web_uri, ln_item.link_to)
    )

    mock_commit.assert_not_called()


def test_commit_no_publish(auth_header):
    publish_id = "11224567-e89b-12d3-a456-426614174000"
    url = "/test/publish/%s/commit" % publish_id
    with TestClient(app) as client:
        # Try to commit non-existent publish
        r = client.post(url, headers=auth_header(roles=["test-publisher"]))

    assert r.status_code == 404
    assert r.json() == {"detail": "No publish found for ID %s" % publish_id}


def test_commit_env_mismatch(auth_header, fake_publish, db):
    """Ensure we can't operate on publishes belonging to other environments"""

    fake_publish.env = "pre"
    db.add(fake_publish)
    db.commit()

    url = "/test/publish/%s/commit" % fake_publish.id
    with TestClient(app) as client:
        r = client.post(url, headers=auth_header(roles=["test-publisher"]))

    assert r.status_code == 404
    assert r.json() == {
        "detail": "No publish found for ID %s" % fake_publish.id
    }


def test_get_publish_typical(auth_header, db):
    """GETing an existing publish returns a publish with no items."""

    publish_id = "11224567-e89b-12d3-a456-426614174000"

    publish = Publish(
        id=publish_id,
        env="test",
        state="PENDING",
        items=[
            Item(
                web_uri="/some/path",
                object_key="1" * 64,
                publish_id=publish_id,
            ),
            Item(
                web_uri="/another/path",
                object_key="2" * 64,
                publish_id=publish_id,
            ),
        ],
    )

    with TestClient(app) as client:
        # Ensure a publish object exists
        db.add(publish)
        db.commit()

        # Try to add some items to it
        r = client.get(
            "/test/publish/%s" % publish_id,
            headers=auth_header(roles=["test-publisher"]),
        )

    # It should have succeeded
    assert r.status_code == 200

    # Returned publish does not contain items
    assert r.json() == {
        "id": "11224567-e89b-12d3-a456-426614174000",
        "env": "test",
        "state": "PENDING",
        "updated": None,
        "links": {
            "self": "/test/publish/11224567-e89b-12d3-a456-426614174000",
            "commit": "/test/publish/11224567-e89b-12d3-a456-426614174000/commit",
        },
        "items": [],
    }


def test_get_publish_not_found(auth_header, fake_publish):
    """GETing a non-existent publish returns an appropriate error message."""

    with TestClient(app) as client:
        r = client.get(
            "/test/publish/%s" % fake_publish.id,
            headers=auth_header(roles=["test-publisher"]),
        )

    # It should have failed
    assert r.status_code == 404
    assert r.json() == {
        "detail": "No publish found for ID %s" % fake_publish.id
    }
