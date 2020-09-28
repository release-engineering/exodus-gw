import pytest

import mock


@pytest.fixture(autouse=True)
def mock_s3_client():
    with mock.patch("aioboto3.Session") as mock_session:
        s3_client = mock.AsyncMock()
        s3_client.__aenter__.return_value = s3_client
        mock_session().client.return_value = s3_client
        yield s3_client
