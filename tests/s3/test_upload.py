import mock
import pytest
from fastapi import HTTPException

from exodus_gw.routers.api import upload

TEST_KEY = "b5bb9d8014a0f9b1d61e21e796d78dccdf1352f23cd32812f4850b878ae4944c"


@pytest.mark.asyncio
async def test_full_upload(mock_s3_client, mock_request_reader):
    """Uploading a complete object is delegated correctly to S3."""

    mock_s3_client.put_object.return_value = {
        "ETag": "a1b2c3",
    }

    request = mock.Mock(
        headers={
            "Content-MD5": "9d0568469d206c1aedf1b71f12f474bc",
            "Content-Length": "10",
        }
    )
    mock_request_reader.return_value = b"some bytes"

    response = await upload(
        request=request,
        env="test",
        key=TEST_KEY,
        uploadId=None,
        partNumber=None,
    )

    # It should delegate request to real S3
    mock_s3_client.put_object.assert_called_once_with(
        Bucket="my-bucket",
        Key=TEST_KEY,
        Body=b"some bytes",
        ContentMD5="9d0568469d206c1aedf1b71f12f474bc",
        ContentLength=10,
    )

    # It should succeed
    assert response.status_code == 200

    # It should return the ETag
    assert response.headers["ETag"] == "a1b2c3"

    # It should have an empty body
    assert response.body == b""


@pytest.mark.asyncio
async def test_part_upload(mock_s3_client, mock_request_reader):
    """Uploading part of an object is delegated correctly to S3."""

    mock_s3_client.upload_part.return_value = {
        "ETag": "aabbcc",
    }

    request = mock.Mock(
        headers={
            "Content-MD5": "e8b7c279de413b7b15f44bf71a796f95",
            "Content-Length": "10",
        }
    )
    mock_request_reader.return_value = b"best bytes"

    response = await upload(
        request=request,
        env="test",
        key=TEST_KEY,
        uploadId="my-best-upload",
        partNumber=88,
    )

    # It should delegate request to real S3
    mock_s3_client.upload_part.assert_called_once_with(
        Bucket="my-bucket",
        Key=TEST_KEY,
        Body=b"best bytes",
        PartNumber=88,
        UploadId="my-best-upload",
        ContentMD5="e8b7c279de413b7b15f44bf71a796f95",
        ContentLength=10,
    )

    # It should succeed
    assert response.status_code == 200

    # It should return the ETag
    assert response.headers["ETag"] == "aabbcc"

    # It should have an empty body
    assert response.body == b""


@pytest.mark.asyncio
async def test_upload_invalid_env(mock_s3_client, mock_request_reader):
    """Uploading to an invalid environment does not delegate to s3."""

    mock_s3_client.put_object.return_value = {
        "ETag": "a1b2c3",
    }

    request = mock.Mock(
        headers={
            "Content-MD5": "9d0568469d206c1aedf1b71f12f474bc",
            "Content-Length": "10",
        }
    )
    mock_request_reader.return_value = b"some bytes"

    with pytest.raises(HTTPException) as err:
        await upload(
            request=request,
            env="foo",
            key=TEST_KEY,
            uploadId=None,
            partNumber=None,
        )

    # It should not delegate request to real S3
    assert not mock_s3_client.put_object.called

    # It should produce an error message
    assert err.value.detail == "Invalid environment='foo'"
