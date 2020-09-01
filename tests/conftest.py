import pytest

import mock


@pytest.fixture(autouse=True)
def mock_s3_client():
    with mock.patch("aioboto3.client") as mock_client:
        s3_client = mock.AsyncMock()
        s3_client.__aenter__.return_value = s3_client
        mock_client.return_value = s3_client
        yield s3_client
