import pytest
from fastapi.testclient import TestClient

from exodus_gw.main import app

TEST_KEY = "b5bb9d8014a0f9b1d61e21e796d78dccdf1352f23cd32812f4850b878ae4944c"


@pytest.mark.asyncio
async def test_full_upload(mock_aws_client, mock_request_reader, auth_header):
    """Uploading a complete object is delegated correctly to S3."""

    mock_request_reader.return_value = b"some bytes"
    mock_aws_client.put_object.return_value = {"ETag": "a1b2c3"}

    with TestClient(app) as client:
        r = client.put(
            "/upload/test/%s" % TEST_KEY,
            headers=auth_header(roles=["test-blob-uploader"]),
        )

    # It should succeed
    assert r.ok

    # It should return the ETag
    assert r.headers["ETag"] == "a1b2c3"

    # It should have an empty body
    assert r.content == b""


@pytest.mark.asyncio
async def test_part_upload(mock_aws_client, mock_request_reader, auth_header):
    """Uploading part of an object is delegated correctly to S3."""

    mock_request_reader.return_value = b"best bytes"
    mock_aws_client.upload_part.return_value = {"ETag": "aabbcc"}

    with TestClient(app) as client:
        r = client.put(
            "/upload/test/%s?uploadId=my-upload&partNumber=88" % TEST_KEY,
            headers=auth_header(roles=["test-blob-uploader"]),
        )

    # It should succeed
    assert r.status_code == 200

    # It should return the ETag
    assert r.headers["ETag"] == "aabbcc"

    # It should have an empty body
    assert r.content == b""
