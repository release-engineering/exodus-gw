import base64
import json
import os
import uuid
from typing import List

import mock
import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm.session import Session

from exodus_gw import auth, database, main, models, schemas, settings  # noqa

from .async_utils import BlockDetector

BASE_QUERY_RESPONSE = {
    "ConsumedCapacity": {
        "CapacityUnits": 1,
        "GlobalSecondaryIndexes": {},
        "LocalSecondaryIndexes": {},
        "ReadCapacityUnits": 0,
        "Table": {
            "CapacityUnits": 0,
            "ReadCapacityUnits": 0,
            "WriteCapacityUnits": 0,
        },
        "TableName": "my-table",
        "WriteCapacityUnits": 0,
    },
    "Count": 0,
    "Items": [],
    "LastEvaluatedKey": {},
    "ScannedCount": 0,
}


@pytest.fixture(autouse=True)
def mock_aws_client():
    with mock.patch("aioboto3.Session") as mock_session:
        aws_client = mock.AsyncMock()
        aws_client.__aenter__.return_value = aws_client
        # This sub-object uses regular methods, not async
        aws_client.meta = mock.MagicMock()
        mock_session().client.return_value = aws_client
        yield aws_client


@pytest.fixture(autouse=True)
def mock_boto3_client():
    with mock.patch("boto3.session.Session") as mock_session:
        client = mock.MagicMock()
        client.query.return_value = BASE_QUERY_RESPONSE
        client.__enter__.return_value = client
        mock_session().client.return_value = client
        yield client


@pytest.fixture()
def mock_request_reader():
    # We don't use the real request reader for these tests as it becomes
    # rather complicated to verify that boto methods were called with the
    # correct expected value. The class is tested separately.
    with mock.patch("exodus_gw.aws.util.RequestReader.get_reader") as m:
        yield m


@pytest.fixture()
def mock_db_session():
    db_session = Session()
    db_session.add = mock.MagicMock()
    db_session.refresh = mock.MagicMock()
    yield db_session


@pytest.fixture(autouse=True)
def sqlite_in_tests(monkeypatch):
    """Any 'real' usage of sqlalchemy during this test suite makes use of
    a fresh sqlite DB for each test run. Tests may either make use of this to
    exercise all the ORM code, or may inject mock DB sessions into endpoints.
    """

    filename = "exodus-gw-test.db"

    try:
        # clean before test
        os.remove(filename)
    except FileNotFoundError:
        # no problem
        pass

    monkeypatch.setenv(
        "EXODUS_GW_DB_URL", "sqlite:///%s?check_same_thread=false" % filename
    )
    yield


@pytest.fixture()
def unmigrated_db():
    """Yields a real DB session configured using current settings.

    Note that this DB is likely to be empty. In the more common case that
    a test wants a DB with all tables in place, use 'db' instead.
    """

    session = Session(bind=database.db_engine(settings.Settings()))
    try:
        yield session
    finally:
        session.close()


@pytest.fixture()
def db(unmigrated_db):
    """Yields a real DB session configured using current settings.

    This session has the schema deployed prior to yielding, so the
    test may assume all tables are already in place.
    """

    with TestClient(main.app):
        pass

    return unmigrated_db


@pytest.fixture(autouse=True, scope="session")
def db_session_block_detector():
    """Wrap DB sessions created by the app with an object to detect
    incorrect async/non-async mixing blocking the main thread.
    """
    old_ctor = main.new_db_session

    def new_ctor(engine):
        real_session = old_ctor(engine)
        return BlockDetector(real_session)

    with mock.patch("exodus_gw.main.new_db_session", new=new_ctor):
        yield


@pytest.fixture()
def fake_publish():
    publish = models.Publish(
        id=uuid.UUID("123e4567-e89b-12d3-a456-426614174000"),
        env="test",
        state="PENDING",
    )
    publish.items = [
        models.Item(
            web_uri="/some/path",
            object_key="0bacfc5268f9994065dd858ece3359fd7a99d82af5be84202b8e84c2a5b07ffa",
            publish_id=publish.id,
        ),
        models.Item(
            web_uri="/other/path",
            object_key="e448a4330ff79a1b20069d436fae94806a0e2e3a6b309cd31421ef088c6439fb",
            publish_id=publish.id,
        ),
        models.Item(
            web_uri="/to/repomd.xml",
            object_key="3f449eb3b942af58e9aca4c1cffdef89c3f1552c20787ae8c966767a1fedd3a5",
            publish_id=publish.id,
        ),
    ]
    yield publish


@pytest.fixture
def auth_header():
    def _auth_header(roles: List[str] = []):
        raw_context = {
            "user": {
                "authenticated": True,
                "internalUsername": "fake-user",
                "roles": roles,
            }
        }

        json_context = json.dumps(raw_context).encode("utf-8")
        b64_context = base64.b64encode(json_context)

        return {"X-RhApiPlatform-CallContext": b64_context.decode("utf-8")}

    return _auth_header
