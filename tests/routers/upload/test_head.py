import pytest
from botocore.exceptions import ClientError

from exodus_gw.routers.upload import head
from exodus_gw.settings import get_environment

TEST_KEY = "b5bb9d8014a0f9b1d61e21e796d78dccdf1352f23cd32812f4850b878ae4944c"


@pytest.mark.asyncio
async def test_head(mock_aws_client):
    """Head request is delegated correctly to S3."""

    mock_aws_client.head_object.return_value = {"ETag": "a1b2c3"}

    response = await head(
        env=get_environment("test"),
        key=TEST_KEY,
    )

    # It should delegate request to real S3
    mock_aws_client.head_object.assert_called_once_with(
        Bucket="my-bucket",
        Key=TEST_KEY,
    )

    # It should succeed
    assert response.status_code == 200

    # Response should contain the ETag
    assert response.headers["ETag"] == "a1b2c3"


@pytest.mark.asyncio
async def test_head_invalid_key(mock_aws_client):
    """Head handles non-2xx responses correctly."""

    mock_aws_client.head_object.side_effect = ClientError(
        {"Error": {"Code": "404"}},
        "HeadObject",
    )

    response = await head(
        env=get_environment("test"),
        key=TEST_KEY,
    )

    # It should delegate request to real S3 without raising exception
    mock_aws_client.head_object.assert_called_once_with(
        Bucket="my-bucket",
        Key=TEST_KEY,
    )

    # It should fail
    assert response.status_code == 404
