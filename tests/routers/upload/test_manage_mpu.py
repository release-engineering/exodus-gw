import textwrap

import mock
import pytest
from fastapi.testclient import TestClient

from exodus_gw.aws.util import xml_response
from exodus_gw.deps import get_environment, get_s3_client, get_settings
from exodus_gw.main import app
from exodus_gw.routers.upload import multipart_upload
from exodus_gw.settings import load_settings

TEST_KEY = "b5bb9d8014a0f9b1d61e21e796d78dccdf1352f23cd32812f4850b878ae4944c"


async def test_create_mpu(mock_aws_client, auth_header):
    """Creating a multipart upload is delegated correctly to S3."""

    mock_aws_client.create_multipart_upload.return_value = {
        "Bucket": "my-bucket",
        "Key": TEST_KEY,
        "UploadId": "my-great-upload",
    }

    with TestClient(app) as client:
        r = client.post(
            "/upload/test/%s?uploads" % TEST_KEY,
            headers=auth_header(roles=["test-blob-uploader"]),
        )

    # It should succeed
    assert r.status_code == 200

    # It should be XML
    assert r.headers["content-type"] == "application/xml"

    # It should include the appropriate data
    expected = xml_response(
        "CreateMultipartUploadOutput",
        Bucket="my-bucket",
        Key=TEST_KEY,
        UploadId="my-great-upload",
    ).body
    assert r.content == expected


async def test_complete_mpu(mock_aws_client):
    """Completing a multipart upload is delegated correctly to S3."""

    mock_aws_client.complete_multipart_upload.return_value = {
        "Location": "https://example.com/some-object",
        "Bucket": "my-bucket",
        "Key": TEST_KEY,
        "ETag": "my-better-etag",
    }

    env = get_environment("test")
    settings = load_settings()

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
    request.app.state.settings = settings
    request.app.state.s3_queues = {}

    s3_client = await get_s3_client(
        request=request, env=env, settings=settings
    ).__anext__()

    response = await multipart_upload(
        request=request,
        env=env,
        s3=s3_client,
        key=TEST_KEY,
        uploadId="my-better-upload",
        uploads=None,
    )

    # It should delegate request to real S3
    mock_aws_client.complete_multipart_upload.assert_called_once_with(
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


async def test_bad_mpu_call(auth_header):
    """Mixing uploadId and uploads arguments gives a validation error."""

    with TestClient(app) as client:
        r = client.post(
            "/upload/test/%s?uploads&uploadId=my-upload" % TEST_KEY,
            headers=auth_header(roles=["test-blob-uploader"]),
        )

    assert r.status_code == 400
    assert r.content == (
        b"<?xml version='1.0' encoding='UTF-8'?>\n"
        b"<Error>"
        b"<Code>400</Code>"
        b"<Message>Invalid uploadId='my-upload', uploads=''</Message>"
        b"<Endpoint>/upload/test/b5bb9d8014a0f9b1d61e21e796d78dccdf1352f23cd32812f4850b878ae4944c</Endpoint>"
        b"</Error>"
    )


async def test_abort_mpu(mock_aws_client, auth_header):
    """Aborting a multipart upload is correctly delegated to S3."""

    with TestClient(app) as client:
        r = client.delete(
            "/upload/test/%s?uploadId=my-lame-upload" % TEST_KEY,
            headers=auth_header(roles=["test-blob-uploader"]),
        )

    # It should be a successful, empty response
    assert r.status_code == 200
    assert r.content == b""
