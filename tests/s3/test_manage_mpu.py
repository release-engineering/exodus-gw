import mock
import pytest
import textwrap

from fastapi import HTTPException

from exodus_gw.s3.api import multipart_upload, abort_multipart_upload, upload
from exodus_gw.s3.util import xml_response

TEST_KEY = "b5bb9d8014a0f9b1d61e21e796d78dccdf1352f23cd32812f4850b878ae4944c"


@pytest.fixture(autouse=True)
def mock_s3_client():
    with mock.patch("aioboto3.client") as mock_client:
        s3_client = mock.AsyncMock()
        s3_client.__aenter__.return_value = s3_client
        mock_client.return_value = s3_client
        yield s3_client


@pytest.mark.asyncio
async def test_create_mpu(mock_s3_client):
    """Creating a multipart upload is delegated correctly to S3."""

    mock_s3_client.create_multipart_upload.return_value = {
        "Bucket": "my-bucket",
        "Key": TEST_KEY,
        "UploadId": "my-great-upload",
    }

    response = await multipart_upload(
        None, bucket="my-bucket", key=TEST_KEY, uploads="",
    )

    # It should delegate request to real S3
    mock_s3_client.create_multipart_upload.assert_called_once_with(
        Bucket="my-bucket", Key=TEST_KEY,
    )

    # It should succeed
    assert response.status_code == 200

    # It should be XML
    assert response.headers["content-type"] == "application/xml"

    # It should include the appropriate data
    expected = xml_response(
        "CreateMultipartUploadOutput",
        Bucket="my-bucket",
        Key=TEST_KEY,
        UploadId="my-great-upload",
    ).body
    assert response.body == expected


@pytest.mark.asyncio
async def test_complete_mpu(mock_s3_client):
    """Completing a multipart upload is delegated correctly to S3."""

    mock_s3_client.complete_multipart_upload.return_value = {
        "Location": "https://example.com/some-object",
        "Bucket": "my-bucket",
        "Key": TEST_KEY,
        "ETag": "my-better-etag",
    }

    # Need some valid request body to complete an MPU
    request = mock.AsyncMock()
    request.body.return_value = textwrap.dedent(
        """
            <?xml version="1.0" encoding="UTF-8"?>
            <CompleteMultipartUpload xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                <Part>
                    <ETag>tagA</ETag>
                    <PartNumber>1</PartNumber>
                </Part>
                <Part>
                    <ETag>tagB</ETag>
                    <PartNumber>2</PartNumber>
                </Part>
            </CompleteMultipartUpload>
        """
    ).strip()

    response = await multipart_upload(
        request=request,
        bucket="my-bucket",
        key=TEST_KEY,
        uploadId="my-better-upload",
        uploads=None,
    )

    # It should delegate request to real S3
    mock_s3_client.complete_multipart_upload.assert_called_once_with(
        Bucket="my-bucket",
        Key=TEST_KEY,
        UploadId="my-better-upload",
        MultipartUpload={
            "Parts": [
                {"ETag": "tagA", "PartNumber": 1},
                {"ETag": "tagB", "PartNumber": 2},
            ]
        },
    )

    # It should succeed
    assert response.status_code == 200

    # It should be XML
    assert response.headers["content-type"] == "application/xml"

    # It should include the appropriate data
    expected = xml_response(
        "CompleteMultipartUploadOutput",
        Location="https://example.com/some-object",
        Bucket="my-bucket",
        Key=TEST_KEY,
        ETag="my-better-etag",
    ).body
    assert response.body == expected


@pytest.mark.asyncio
async def test_bad_mpu_call(mock_s3_client):
    """Mixing uploadId and uploads arguments gives a validation error."""

    with pytest.raises(HTTPException) as exc_info:
        await multipart_upload(
            request=None,
            bucket="my-bucket",
            key=TEST_KEY,
            uploadId="oops",
            uploads="not valid to mix these args",
        )

    assert exc_info.value.status_code == 400


@pytest.mark.asyncio
async def test_abort_mpu(mock_s3_client):
    """Aborting a multipart upload is correctly delegated to S3."""

    response = await abort_multipart_upload(
        bucket="my-bucket", key=TEST_KEY, uploadId="my-lame-upload",
    )

    # It should delegate the request to real S3
    mock_s3_client.abort_multipart_upload.assert_called_once_with(
        Bucket="my-bucket", Key=TEST_KEY, UploadId="my-lame-upload",
    )

    # It should be a successful, empty response
    assert response.status_code == 200
    assert response.body == b""
