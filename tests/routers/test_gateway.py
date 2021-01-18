from types import GeneratorType

import pytest
from fastapi import HTTPException
from mock import MagicMock

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


def test_whoami():
    # All work is done by fastapi deserialization, so this doesn't actually
    # do anything except return the passed object.
    context = object()
    assert gateway.whoami(context=context) is context


def test_get_db(monkeypatch) -> None:
    monkeypatch.setattr("exodus_gw.routers.gateway.SessionLocal", MagicMock())

    db = gateway.get_db()
    assert isinstance(db, GeneratorType)

    for session in db:
        assert isinstance(session, MagicMock)
