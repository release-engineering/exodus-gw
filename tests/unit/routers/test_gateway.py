from types import GeneratorType

import pytest
from mock import MagicMock

from exodus_gw.models import Publish
from exodus_gw.routers import gateway


def test_healthcheck():
    assert gateway.healthcheck() == {"detail": "exodus-gw is running"}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "env",
    [
        "dev",
        "qa",
        "stage",
        "prod",
    ],
)
async def test_publish_env_exists(env, mock_db_session):
    publish = await gateway.publish(env=env, db=mock_db_session)
    assert isinstance(publish, Publish)


@pytest.mark.asyncio
async def test_publish_env_doesnt_exist(mock_db_session):
    env = "env_doesnt_exist"
    publish = await gateway.publish(env=env, db=mock_db_session)
    assert publish == {"error": "environment {0} not found".format(env)}


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
