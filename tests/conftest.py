import mock
import pytest
from mock import MagicMock
from sqlalchemy.orm.session import Session

from exodus_gw import schemas


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
    items = []
    items.append(schemas.ItemBase(uri="/some/path", object_key="abcde"))
    items.append(schemas.ItemBase(uri="/other/path", object_key="a1b2"))
    return items
