from types import GeneratorType

import mock
import pytest
from fastapi import HTTPException

from exodus_gw import models
from exodus_gw.routers import gateway


def test_healthcheck():
    assert gateway.healthcheck() == {"detail": "exodus-gw is running"}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "env",
    [
        "test",
        "test2",
        "test3",
    ],
)
async def test_publish_env_exists(env, mock_db_session):
    publish = await gateway.publish(env=env, db=mock_db_session)
    assert isinstance(publish, models.Publish)


@pytest.mark.asyncio
async def test_publish_env_doesnt_exist(mock_db_session):
    env = "foo"
    with pytest.raises(HTTPException) as exc_info:
        await gateway.publish(env=env, db=mock_db_session)
    assert exc_info.value.status_code == 404
    assert exc_info.value.detail == "Invalid environment='foo'"


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
    # Simulate single item to "test3" environment to test list coersion.
    items = mock_item_list[0] if env == "test3" else mock_item_list

    assert (
        await gateway.update_publish_items(
            env=env,
            publish_id=publish_id,
            items=items,
            db=mock_db_session,
        )
        == {}
    )


@pytest.mark.asyncio
async def test_update_publish_items_env_doesnt_exist(
    mock_db_session, mock_item_list
):
    env = "foo"
    publish_id = "123e4567-e89b-12d3-a456-426614174000"

    with pytest.raises(HTTPException) as exc_info:
        await gateway.update_publish_items(
            env=env,
            publish_id=publish_id,
            items=mock_item_list,
            db=mock_db_session,
        )

    assert exc_info.value.status_code == 404
    assert exc_info.value.detail == "Invalid environment='foo'"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "env",
    [
        "test",
        "test2",
        "test3",
    ],
)
@mock.patch("exodus_gw.routers.gateway.get_publish_by_id")
async def test_commit_publish(
    mock_get_publish, env, mock_aws_client, mock_publish, mock_db_session
):
    mock_get_publish.return_value = mock_publish
    mock_aws_client.batch_write_item.return_value = {"UnprocessedItems": {}}

    assert (
        await gateway.commit_publish(
            env=env, publish_id=mock_publish.id, db=mock_db_session
        )
        == {}
    )


@pytest.mark.asyncio
@mock.patch("exodus_gw.routers.gateway.get_publish_by_id")
async def test_commit_publish_env_doesnt_exist(
    mock_get_publish, mock_aws_client, mock_publish, mock_db_session
):
    mock_get_publish.return_value = mock_publish
    mock_aws_client.batch_write_item.return_value = {"UnprocessedItems": {}}

    env = "foo"

    with pytest.raises(HTTPException) as exc_info:
        await gateway.commit_publish(
            env=env, publish_id=mock_publish.id, db=mock_db_session
        )

    assert exc_info.value.status_code == 404
    assert exc_info.value.detail == "Invalid environment='foo'"


@pytest.mark.asyncio
@mock.patch("exodus_gw.routers.gateway.batch_write")
@mock.patch("exodus_gw.routers.gateway.get_publish_by_id")
async def test_commit_publish_write_failed(
    mock_get_publish, mock_batch_write, mock_publish, mock_db_session, caplog
):
    mock_get_publish.return_value = mock_publish

    fake_responses = {
        "puts": {
            "UnprocessedItems": {
                "my-table": [
                    {
                        "PutRequest": {
                            "Item": {
                                "web_uri": "/some/path",
                                "object_key": "abcde",
                                "from_date": "2021-01-01T00:00:00.0",
                            }
                        }
                    },
                    {
                        "PutRequest": {
                            "Item": {
                                "web_uri": "/other/path",
                                "object_key": "a1b2",
                                "from_date": "2021-01-01T00:00:00.0",
                            }
                        }
                    },
                ]
            }
        },
        "deletes": {
            "UnprocessedItems": {
                "my-table": [
                    {
                        "DeleteRequest": {
                            "Key": {
                                "web_uri": "/some/path",
                                "object_key": "abcde",
                                "from_date": "2021-01-01T00:00:00.0",
                            }
                        }
                    },
                    {
                        "DeleteRequest": {
                            "Key": {
                                "web_uri": "/other/path",
                                "object_key": "a1b2",
                                "from_date": "2021-01-01T00:00:00.0",
                            }
                        }
                    },
                ]
            }
        },
    }

    # Fail with successful cleanup.
    mock_batch_write.side_effect = [
        # Put unsuccessful.
        fake_responses["puts"],
        # Delete successful.
        {"UnprocessedItems": {}},
    ]

    await gateway.commit_publish(
        env="test", publish_id=mock_publish.id, db=mock_db_session
    )

    # Fail with unsuccessful cleanup.
    mock_batch_write.side_effect = [
        # Put unsuccessful.
        fake_responses["puts"],
        # Delete unsuccessful.
        fake_responses["deletes"],
    ]

    with pytest.raises(RuntimeError) as exc_info:
        await gateway.commit_publish(
            env="test", publish_id=mock_publish.id, db=mock_db_session
        )

    assert (
        "Unprocessed items:\n\t%s"
        % str(fake_responses["deletes"]["UnprocessedItems"])
        in caplog.text
    )
    assert "Cleanup failed" in str(exc_info.value)


def test_whoami():
    # All work is done by fastapi deserialization, so this doesn't actually
    # do anything except return the passed object.
    context = object()
    assert gateway.whoami(context=context) is context


def test_get_db(monkeypatch) -> None:
    monkeypatch.setattr(
        "exodus_gw.routers.gateway.SessionLocal", mock.MagicMock()
    )

    db = gateway.get_db()
    assert isinstance(db, GeneratorType)

    for session in db:
        assert isinstance(session, mock.MagicMock)
