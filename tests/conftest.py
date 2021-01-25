import os

import dramatiq
import mock
import pytest
from mock import MagicMock
from sqlalchemy.orm.session import Session

# This must happen early during tests, prior to import of
# anything from the exodus_gw.worker module.
os.environ["EXODUS_GW_STUB_BROKER"] = "1"

from exodus_gw import models, schemas  # noqa


@pytest.fixture(autouse=True)
def mock_aws_client():
    with mock.patch("aioboto3.Session") as mock_session:
        aws_client = mock.AsyncMock()
        aws_client.__aenter__.return_value = aws_client
        # This sub-object uses regular methods, not async
        aws_client.meta = mock.MagicMock()
        mock_session().client.return_value = aws_client
        yield aws_client


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
    db_session.add = MagicMock()
    db_session.refresh = MagicMock()
    yield db_session


@pytest.fixture()
def mock_item_list():
    return [
        schemas.ItemBase(
            web_uri="/some/path",
            object_key="abcde",
            from_date="2021-01-01T00:00:00.0",
        ),
        schemas.ItemBase(
            web_uri="/other/path",
            object_key="a1b2",
            from_date="2021-01-01T00:00:00.0",
        ),
        schemas.ItemBase(
            web_uri="/to/repomd.xml",
            object_key="c3d4",
            from_date="2021-01-01T00:00:00.0",
        ),
    ]


@pytest.fixture()
def mock_publish(mock_item_list):
    publish = models.Publish()
    publish.id = "123e4567-e89b-12d3-a456-426614174000"
    publish.items = [
        models.Item(**item.dict(), publish_id=publish.id)
        for item in mock_item_list
    ]
    yield publish


@pytest.fixture()
def stub_broker():
    broker = dramatiq.get_broker()

    # discards any messages in progress at start of next test.
    broker.flush_all()

    return broker


@pytest.fixture()
def stub_worker(stub_broker):
    # runs a worker connected to the stub broker, so messages
    # can be handled in-process during test execution
    worker = dramatiq.Worker(stub_broker, worker_timeout=100)
    worker.start()
    yield worker
    worker.stop(timeout=5000)
