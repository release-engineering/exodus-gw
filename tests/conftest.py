import pytest

import mock


@pytest.fixture(autouse=True)
def mock_s3_client():
    with mock.patch("aioboto3.Session") as mock_session:
        s3_client = mock.AsyncMock()
        s3_client.__aenter__.return_value = s3_client
        # This sub-object uses regular methods, not async
        s3_client.meta = mock.MagicMock()
        mock_session().client.return_value = s3_client
        yield s3_client


@pytest.fixture()
def mock_request_reader():
    # We don't use the real request reader for these tests as it becomes
    # rather complicated to verify that boto methods were called with the
    # correct expected value. The class is tested separately.
    with mock.patch("exodus_gw.s3.util.RequestReader.get_reader") as m:
        yield m
