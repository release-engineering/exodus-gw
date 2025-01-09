import base64
import gzip
import json
import os
from datetime import datetime
from typing import Any

import dramatiq
import mock
import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm.session import Session

from exodus_gw import database, main, models, settings  # noqa
from exodus_gw.dramatiq import Broker

from .async_utils import BlockDetector

DEFAULT_EXCLUDE_PATHS = ["/files/", "/images/", "/iso/"]


async def fake_aexit_instancemethod(self, exc_type, exc_val, exc_tb):
    pass


@pytest.fixture(autouse=True)
def mock_aws_client():
    with mock.patch("aioboto3.Session") as mock_session:
        aws_client = mock.AsyncMock()
        aws_client.__aenter__.return_value = aws_client
        aws_client.__aexit__ = fake_aexit_instancemethod
        # This sub-object uses regular methods, not async
        aws_client.meta = mock.MagicMock()
        mock_session().client.return_value = aws_client
        yield aws_client


@pytest.fixture(params=["binary-config", "text-config"])
def fake_dynamodb_query(
    fake_config: dict[str, Any], request: pytest.FixtureRequest
):
    binary_config = request.param == "binary-config"

    # Returns a callable which can be used as a mock side-effect
    # to make a DynamoDB query on exodus-config return the current
    # fake_config.
    def side_effect(
        TableName,
        Limit,
        ScanIndexForward,
        KeyConditionExpression,
        ExpressionAttributeValues,
    ):
        # This is the only query we expect right now.
        assert TableName == "my-config"
        assert Limit == 1
        config_json = json.dumps(fake_config)
        if binary_config:
            config_value = {"B": gzip.compress(config_json.encode())}
        else:
            config_value = {"S": config_json}
        return {
            "Count": 1,
            "Items": [{"config": config_value}],
        }

    return side_effect


@pytest.fixture(autouse=True)
def mock_boto3_session():
    with mock.patch("boto3.session.Session") as mock_session:
        yield mock_session


@pytest.fixture()
def mock_boto3_client(fake_dynamodb_query, mock_boto3_session):
    client = mock.MagicMock()
    client.query.side_effect = fake_dynamodb_query
    client.__enter__.return_value = client
    mock_boto3_session().client.return_value = client
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


@pytest.fixture(autouse=True)
def sqlite_broker_in_tests(sqlite_in_tests):
    """Reset dramatiq broker during test, after settings have been updated to point
    at sqlite DB.

    This is required because the dramatiq design is such that a broker needs to be
    installed at import-time, and actors declared using the decorator will be pointing
    at that broker. Since that happens too early for us to set up our test fixtures,
    we need to reset the broker created at import time to point at our test DB.
    """

    broker = dramatiq.get_broker()
    assert isinstance(broker, Broker)
    broker.reset()


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
        id="123e4567-e89b-12d3-a456-426614174000",
        env="test",
        state="PENDING",
    )
    publish.items = [
        models.Item(
            web_uri="/some/path",
            object_key="0bacfc5268f9994065dd858ece3359fd7a99d82af5be84202b8e84c2a5b07ffa",
            publish_id=publish.id,
            updated=datetime(2023, 10, 4, 3, 52, 0),
        ),
        models.Item(
            web_uri="/other/path",
            object_key="e448a4330ff79a1b20069d436fae94806a0e2e3a6b309cd31421ef088c6439fb",
            publish_id=publish.id,
            updated=datetime(2023, 10, 4, 3, 52, 1),
        ),
        models.Item(
            web_uri="/content/testproduct/1/repo/repomd.xml",
            object_key="3f449eb3b942af58e9aca4c1cffdef89c3f1552c20787ae8c966767a1fedd3a5",
            publish_id=publish.id,
            updated=datetime(2023, 10, 4, 3, 52, 2),
        ),
        models.Item(
            web_uri="/content/testproduct/1/repo/.__exodus_autoindex",
            object_key="5891b5b522d5df086d0ff0b110fbd9d21bb4fc7163af34d08286a2e846f6be03",
            publish_id=publish.id,
            updated=datetime(2023, 10, 4, 3, 52, 2),
        ),
    ]
    yield publish


@pytest.fixture
def auth_header():
    def _auth_header(roles: list[str] = []):
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


@pytest.fixture
def dummy_private_key():
    return """-----BEGIN RSA PRIVATE KEY-----
MIICWgIBAAKBgEku7kJh8jDweJCO73COmlSKlcw/A55kWLt245m0sQzx5P9eF3jG
NiDxYb9WZShyeckoS9B6i8+zX6g8OcnKmLXuavHyJpQXmE01ZpizCJiTcn7ihw/n
tPvzc+Ty1Haea30RPUvRUuhaqV+RjXSzCnTRkNiqH6YXLYbUIgfXN1rXAgMBAAEC
gYAkNCBQHK44ga3TLbLMBu/YJNroOMAsik3PJ4h+0IHJ+pyjrEOGTuAWOfN2OWI/
uSoAVnvy/bzOmlkXG/wmlKAo0QCDhieWM2Ss+rIkBjmSX8yO+K41Mu+BwOLS/Ynb
ch119R8L+TBS0pGt2tDBr5c+DJfDqcS+lhRJgoTenWkZ0QJBAIsxHUNyZV81mTP2
V5J0kViF/dtRDzQTjTvumWHcDj5R3VuQMrxQJS+8GTYO+6xP+W+oZIIY0TVUhuHg
WUb8q08CQQCGmQ/LnljQim73iSs6ew5VcIVghcMqhlXhZ6+LR0g7A0T2gNTjrGsS
UY9gdLOIpNFfWeUtWnTf7g3YUp41VNX5AkAJIFJD3tdIs9H0tz0srBnvjPGFFL6D
cpi7CjziTrRcX6+81iqNcE/P3mxkv/y+Yov/RzI32Xq2HXGuk7Am2GA/AkBO65J6
ZsdWx8TW+aPSL3MxH7/k36mW1pumheBFPy+YAou+Kb4qHN/PJul1uhfG6DUnvpMF
K8PZxUBy9cZ0KOEpAkA1b7cZpW40ZowMvAH6sF+7Ok1NFd+08AMXLiSJ6z7Sk29s
UrfAc2T6ZnfNC4qLIaDyo87CzVG/wk1Upr21z0YD
-----END RSA PRIVATE KEY-----"""


@pytest.fixture
def fake_config():
    return {
        "listing": {
            "/content/dist/rhel/server": {
                "values": ["8"],
                "var": "releasever",
            },
            "/content/dist/rhel/server/8": {
                "values": ["x86_64"],
                "var": "basearch",
            },
        },
        "origin_alias": [
            {
                "src": "/content/origin",
                "dest": "/origin",
                "exclude_paths": DEFAULT_EXCLUDE_PATHS,
            },
            {
                "src": "/origin/rpm",
                "dest": "/origin/rpms",
                "exclude_paths": DEFAULT_EXCLUDE_PATHS,
            },
        ],
        "releasever_alias": [
            {
                "src": "/content/dist/rhel8/8",
                "dest": "/content/dist/rhel8/8.5",
                "exclude_paths": DEFAULT_EXCLUDE_PATHS,
            },
            {
                "src": "/content/testproduct/1",
                "dest": "/content/testproduct/1.1.0",
                "exclude_paths": DEFAULT_EXCLUDE_PATHS,
            },
            {
                "src": "/content/product_duplicate/1",
                "dest": "/content/testproduct/1.1.0",
                "exclude_paths": DEFAULT_EXCLUDE_PATHS,
            },
        ],
        "rhui_alias": [
            {
                "src": "/content/dist/rhel8/rhui",
                "dest": "/content/dist/rhel8",
                "exclude_paths": DEFAULT_EXCLUDE_PATHS,
            },
            {
                "src": "/content/testproduct/rhui",
                "dest": "/content/testproduct",
                "exclude_paths": DEFAULT_EXCLUDE_PATHS,
            },
        ],
    }
