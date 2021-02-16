import uuid

import mock
import pytest
from fastapi.testclient import TestClient

from exodus_gw import routers, schemas
from exodus_gw.main import app
from exodus_gw.models import Publish
from exodus_gw.settings import Environment, Settings


@pytest.mark.parametrize(
    "env",
    [
        "test",
        "test2",
        "test3",
    ],
)
def test_publish_env_exists(env, db):
    with TestClient(app) as client:
        r = client.post("/%s/publish" % env)

    # Should succeed
    assert r.ok

    # Should have returned a publish object
    publish_id = r.json()["id"]

    publishes = db.query(Publish).filter(Publish.id == publish_id)
    assert publishes.count() == 1


def test_publish_env_doesnt_exist():
    with TestClient(app) as client:
        r = client.post("/foo/publish")

    # It should fail
    assert r.status_code == 404

    # It should mention that it was a bad environment
    assert r.json() == {"detail": "Invalid environment='foo'"}


@pytest.mark.asyncio
async def test_publish_links(mock_db_session):
    publish = await routers.publish.publish(
        env=Environment("test", "some-profile", "some-bucket", "some-table"),
        db=mock_db_session,
    )

    # The schema (realistic result) resulting from the publish
    # should contain accurate links.
    assert schemas.Publish(**publish.__dict__).links == {
        "self": "/test/publish/%s" % publish.id,
        "commit": "/test/publish/%s/commit" % publish.id,
    }


def test_update_publish_items_typical(db):
    """PUTting some items on a publish creates expected objects in DB."""

    publish_id = "11224567-e89b-12d3-a456-426614174000"

    publish = Publish(id=uuid.UUID("{%s}" % publish_id), env="test")

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
                    "from_date": "date1",
                },
                {
                    "web_uri": "/uri2",
                    "object_key": "2" * 64,
                    "from_date": "date2",
                },
            ],
        )

    # It should have succeeded
    assert r.ok

    # publish object should now have matching items
    db.refresh(publish)

    # (note: ignoring from_date because it's planned for removal from the request format)
    items = sorted(publish.items, key=lambda item: item.web_uri)
    item_dicts = [
        {"web_uri": item.web_uri, "object_key": item.object_key}
        for item in items
    ]

    # Should have stored exactly what we asked for
    assert item_dicts == [
        {"web_uri": "/uri1", "object_key": "1" * 64},
        {"web_uri": "/uri2", "object_key": "2" * 64},
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "env",
    [
        "test",
        "test2",
        "test3",
    ],
)
async def test_update_publish_items_env_exists(env, mock_db_session):
    test_items = [
        schemas.ItemBase(
            web_uri="/some/path",
            object_key="0bacfc5268f9994065dd858ece3359fd7a99d82af5be84202b8e84c2a5b07ffa",
            from_date="2021-01-01T00:00:00.0",
        ),
        schemas.ItemBase(
            web_uri="/other/path",
            object_key="e448a4330ff79a1b20069d436fae94806a0e2e3a6b309cd31421ef088c6439fb",
            from_date="2021-01-01T00:00:00.0",
        ),
        schemas.ItemBase(
            web_uri="/to/repomd.xml",
            object_key="3f449eb3b942af58e9aca4c1cffdef89c3f1552c20787ae8c966767a1fedd3a5",
            from_date="2021-01-01T00:00:00.0",
        ),
    ]
    publish_id = "123e4567-e89b-12d3-a456-426614174000"
    # Simulate single item to "test3" environment to test list coercion.
    items = test_items[0] if env == "test3" else test_items

    env = Environment(env, "test-profile", "test-bucket", "test-table")

    assert (
        await routers.publish.update_publish_items(
            env=env,
            publish_id=publish_id,
            items=items,
            db=mock_db_session,
        )
        == {}
    )


@mock.patch("exodus_gw.worker.commit")
def test_commit_publish(mock_commit, fake_publish, mock_db_session):
    """Ensure commit_publish delegates to worker correctly"""

    env = Environment("test", "some-profile", "some-bucket", "some-table")

    routers.publish.commit_publish(
        env=env,
        publish_id=fake_publish.id,
        db=mock_db_session,
        settings=Settings(),
    )

    mock_commit.assert_has_calls(
        calls=[
            mock.call.send(
                publish_id="123e4567-e89b-12d3-a456-426614174000", env="test"
            )
        ],
    )
