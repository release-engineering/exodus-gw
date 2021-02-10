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
async def test_update_publish_items_env_exists(
    env, mock_db_session, mock_item_list
):
    publish_id = "123e4567-e89b-12d3-a456-426614174000"
    # Simulate single item to "test3" environment to test list coercion.
    items = mock_item_list[0] if env == "test3" else mock_item_list

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
def test_commit_publish(mock_commit, mock_publish, mock_db_session):
    """Ensure commit_publish delegates to worker correctly"""

    env = Environment("test", "some-profile", "some-bucket", "some-table")

    routers.publish.commit_publish(
        env=env,
        publish_id=mock_publish.id,
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
