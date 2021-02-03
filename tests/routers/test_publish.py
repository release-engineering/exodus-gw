import mock
import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from exodus_gw import models, routers, schemas
from exodus_gw.database import SessionLocal
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
def test_publish_env_exists(env):
    with TestClient(app) as client:
        r = client.post("/%s/publish" % env)

    # Should succeed
    assert r.ok

    # Should have returned a publish object
    publish_id = r.json()["id"]

    db = SessionLocal()
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


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "env",
    [
        "test",
        "test2",
        "test3",
    ],
)
@mock.patch("exodus_gw.routers.publish.write_batches")
@mock.patch("exodus_gw.routers.publish.get_publish_by_id")
async def test_commit_publish(
    mock_get_publish,
    mock_write_batches,
    env,
    mock_publish,
    mock_db_session,
):
    mock_get_publish.return_value = mock_publish
    mock_write_batches.return_value = True

    assert (
        await routers.publish.commit_publish(
            env=env,
            publish_id=mock_publish.id,
            db=mock_db_session,
            settings=Settings(),
        )
        == {}
    )
    # Should write repomd.xml file separately after other items.
    mock_write_batches.assert_has_calls(
        calls=[
            mock.call(env, mock_publish.items[:2]),
            mock.call(env, [mock_publish.items[2]]),
        ],
        any_order=False,
    )


@pytest.mark.asyncio
@mock.patch("exodus_gw.routers.publish.get_publish_by_id")
async def test_commit_publish_env_doesnt_exist(mock_publish, mock_db_session):
    env = "foo"

    with pytest.raises(HTTPException) as exc_info:
        await routers.publish.commit_publish(
            env=env,
            publish_id=mock_publish.id,
            db=mock_db_session,
            settings=Settings(),
        )

    assert exc_info.value.status_code == 404
    assert exc_info.value.detail == "Invalid environment='foo'"


@pytest.mark.asyncio
@mock.patch("exodus_gw.routers.publish.write_batches")
@mock.patch("exodus_gw.routers.publish.get_publish_by_id")
async def test_commit_publish_write_failed(
    mock_get_publish, mock_write_batches, mock_publish, mock_db_session
):
    mock_get_publish.return_value = mock_publish
    mock_write_batches.side_effect = [False, True]

    await routers.publish.commit_publish(
        env="test",
        publish_id=mock_publish.id,
        db=mock_db_session,
        settings=Settings(),
    )

    mock_write_batches.assert_has_calls(
        calls=[
            mock.call("test", mock_publish.items[:2]),
            mock.call("test", mock_publish.items[:2], delete=True),
        ],
        any_order=False,
    )


@pytest.mark.asyncio
@mock.patch("exodus_gw.routers.publish.write_batches")
@mock.patch("exodus_gw.routers.publish.get_publish_by_id")
async def test_commit_publish_entry_point_files_failed(
    mock_get_publish, mock_write_batches, mock_publish, mock_db_session
):
    mock_get_publish.return_value = mock_publish
    mock_write_batches.side_effect = [True, False, True]

    await routers.publish.commit_publish(
        env="test",
        publish_id=mock_publish.id,
        db=mock_db_session,
        settings=Settings(),
    )

    mock_write_batches.assert_has_calls(
        calls=[
            mock.call("test", mock_publish.items[:2]),
            mock.call("test", [mock_publish.items[2]]),
            mock.call("test", mock_publish.items, delete=True),
        ],
        any_order=False,
    )
