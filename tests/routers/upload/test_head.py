import json

from botocore.exceptions import ClientError
from fastapi.testclient import TestClient

from exodus_gw.main import app

TEST_KEY = "b5bb9d8014a0f9b1d61e21e796d78dccdf1352f23cd32812f4850b878ae4944c"


async def test_head(mock_aws_client, auth_header, monkeypatch):
    """Head request is delegated correctly to S3."""

    monkeypatch.setenv(
        "EXODUS_GW_UPLOAD_META_FIELDS",
        json.dumps(
            {
                # md5sum of content being migrated
                "exodus-migration-md5": "^[0-9a-f]{32}$",
                # original source of migrated content
                "exodus-migration-src": "^.{1,2000}$",
            }
        ),
    )

    mock_aws_client.head_object.return_value = {
        "ETag": "a1b2c3",
        "Metadata": {
            "exodus-migration-md5": "94e19d5d30b26306167e9e7bae6b28fd",
            "exodus-migration-src": "original/source",
        },
    }

    with TestClient(app) as client:
        r = client.head(
            "/upload/test/%s" % TEST_KEY,
            headers=auth_header(roles=["test-blob-uploader"]),
        )

    assert r.status_code == 200
    assert r.headers["etag"] == "a1b2c3"
    assert (
        r.headers["x-amz-meta-exodus-migration-md5"]
        == "94e19d5d30b26306167e9e7bae6b28fd"
    )
    assert r.headers["x-amz-meta-exodus-migration-src"] == "original/source"
    assert r.headers["content-length"] == "0"
    assert r.headers["x-request-id"]


async def test_head_nonexistent_key(mock_aws_client, auth_header):
    """Head handles 404 responses correctly."""

    mock_aws_client.head_object.side_effect = ClientError(
        {
            "Error": {
                "Code": "NoSuchKey",
                "Message": "The resource you requested does not exist",
            },
            "ResponseMetadata": {
                "HTTPStatusCode": 404,
            },
        },
        "HeadObject",
    )

    with TestClient(app) as client:
        r = client.head(
            "/upload/test/%s" % TEST_KEY,
            headers=auth_header(roles=["test-blob-uploader"]),
        )

    assert r.status_code == 404
