import uuid

import mock
import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from exodus_gw import routers, schemas
from exodus_gw.main import app
from exodus_gw.models import Item, Publish, Task
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
    assert r.ok

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

    publish = Publish(
        id=uuid.UUID("{%s}" % publish_id), env="test", state="PENDING"
    )

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
                {
                    "web_uri": "/uri2",
                    "object_key": "2" * 64,
                },
                {
                    "web_uri": "/uri3",
                    "link_to": "/uri1",
                },
            ],
            headers=auth_header(roles=["test-publisher"]),
        )

    # It should have succeeded
    assert r.ok

    # publish object should now have matching items
    db.refresh(publish)

    items = sorted(publish.items, key=lambda item: item.web_uri)
    item_dicts = [
        {
            "web_uri": item.web_uri,
            "object_key": item.object_key,
            "link_to": item.link_to,
        }
        for item in items
    ]

    # Should have stored exactly what we asked for
    assert item_dicts == [
        {"web_uri": "/uri1", "object_key": "1" * 64, "link_to": ""},
        {"web_uri": "/uri2", "object_key": "2" * 64, "link_to": ""},
        {"web_uri": "/uri3", "object_key": "", "link_to": "/uri1"},
    ]


def test_update_publish_items_single_item(db, auth_header):
    """PUTting a single item on a publish creates expected object in DB."""

    publish_id = "11224567-e89b-12d3-a456-426614174000"

    publish = Publish(
        id=uuid.UUID("{%s}" % publish_id), env="test", state="PENDING"
    )

    with TestClient(app) as client:
        # ensure a publish object exists
        db.add(publish)
        db.commit()

        # Try to add an item to it
        r = client.put(
            "/test/publish/%s" % publish_id,
            json={
                "web_uri": "/uri1",
                "object_key": "1" * 64,
            },
            headers=auth_header(roles=["test-publisher"]),
        )

    # It should have succeeded
    assert r.ok

    # publish object should now have a matching item
    db.refresh(publish)

    item_dicts = [
        {
            "web_uri": item.web_uri,
            "object_key": item.object_key,
            "link_to": item.link_to,
        }
        for item in publish.items
    ]

    # Should have stored exactly what we asked for
    assert item_dicts == [
        {"web_uri": "/uri1", "object_key": "1" * 64, "link_to": ""}
    ]


def test_update_publish_items_invalid_publish(db, auth_header):
    """PUTting items on a completed publish fails with code 409."""

    publish_id = "11224567-e89b-12d3-a456-426614174000"

    publish = Publish(
        id=uuid.UUID("{%s}" % publish_id), env="test", state="COMPLETE"
    )

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


def test_update_publish_items_invalid_item(db, auth_header):
    """PUTting an item without object_key or link_to fails with code 400."""

    publish_id = "11224567-e89b-12d3-a456-426614174000"

    publish = Publish(
        id=uuid.UUID("{%s}" % publish_id), env="test", state="PENDING"
    )

    with TestClient(app) as client:
        # ensure a publish object exists
        db.add(publish)
        db.commit()

        # Try to add an item to it
        r = client.put(
            "/test/publish/%s" % publish_id,
            json={
                "web_uri": "/uri1",
            },
            headers=auth_header(roles=["test-publisher"]),
        )

    # It should have failed with 400
    assert r.status_code == 400
    assert r.json() == {"detail": "No object key or link target for '/uri1'"}


def test_update_publish_items_invalid_link(db, auth_header):
    """PUTting an item with a non-absolute link_to fails with code 400."""

    publish_id = "11224567-e89b-12d3-a456-426614174000"

    publish = Publish(
        id=uuid.UUID("{%s}" % publish_id), env="test", state="PENDING"
    )

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
                    "link_to": "/../../uri1",
                },
            ],
            headers=auth_header(roles=["test-publisher"]),
        )

    # It should have failed with 400
    assert r.status_code == 400
    assert r.json() == {
        "detail": "Link target is not an absolute path: '/../../uri1'"
    }


@mock.patch("exodus_gw.worker.commit")
def test_commit_publish(mock_commit, fake_publish, db):
    """Ensure commit_publish delegates to worker correctly and creates task."""

    db.add(fake_publish)
    db.commit()

    publish_task = routers.publish.commit_publish(
        env=get_environment("test"),
        publish_id=fake_publish.id,
        db=db,
        settings=Settings(),
    )

    assert isinstance(publish_task, Task)

    mock_commit.assert_has_calls(
        calls=[
            mock.call.send(
                publish_id="123e4567-e89b-12d3-a456-426614174000",
                env="test",
                from_date=mock.ANY,
            )
        ],
    )


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

    # Add an item with link to an existing item.
    ln_item = Item(
        web_uri="/alternate/route/to/some/path",
        object_key="",
        link_to="/some/path",
        publish_id=fake_publish.id,
    )
    fake_publish.items.append(ln_item)

    db.add(fake_publish)
    db.commit()

    publish_task = routers.publish.commit_publish(
        env=get_environment("test"),
        publish_id=fake_publish.id,
        db=db,
        settings=Settings(),
    )

    # Should've filled item's object_key with known value.
    assert ln_item.object_key == (
        "0bacfc5268f9994065dd858ece3359fd7a99d82af5be84202b8e84c2a5b07ffa"
    )
    # Should've created and sent task.
    assert isinstance(publish_task, Task)

    mock_commit.assert_has_calls(
        calls=[
            mock.call.send(
                publish_id="123e4567-e89b-12d3-a456-426614174000",
                env="test",
                from_date=mock.ANY,
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
        )

    assert exc_info.value.status_code == 400
    assert (
        exc_info.value.detail
        == "Unable to resolve item object_key:\n\tURI: '%s'\n\tLink: '%s'"
        % (ln_item.web_uri, ln_item.link_to)
    )

    mock_commit.assert_not_called()
