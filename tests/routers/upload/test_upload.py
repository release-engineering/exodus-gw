import mock
import pytest
from botocore.exceptions import ClientError
from fastapi.testclient import TestClient

from exodus_gw.deps import get_environment, get_s3_client
from exodus_gw.main import app
from exodus_gw.routers.upload import _already_uploaded, _ensure_aborted
from exodus_gw.settings import load_settings

TEST_KEY = "b5bb9d8014a0f9b1d61e21e796d78dccdf1352f23cd32812f4850b878ae4944c"


async def test_full_upload(
    mock_aws_client, mock_request_reader, auth_header, monkeypatch
):
    """Uploading a complete object is delegated correctly to S3."""

    monkeypatch.setenv(
        "EXODUS_GW_UPLOAD_META_FIELDS",
        '{"exodus-migration-md5": "^[0-9a-f]{32}$","exodus-migration-src": "^.{1,2000}$"}',
    )

    headers = {
        **auth_header(roles=["test-blob-uploader"]),
        "x-amz-meta-exodus-migration-md5": "94e19d5d30b26306167e9e7bae6b28fd",
        "x-amz-meta-exodus-migration-src": "original/source",
    }

    mock_request_reader.return_value = b"some bytes"
    mock_aws_client.put_object.return_value = {"ETag": "a1b2c3"}

    with TestClient(app) as client:
        r = client.put("/upload/test/%s" % TEST_KEY, headers=headers)

    mock_aws_client.put_object.assert_called_with(
        Bucket="my-bucket",
        Key=TEST_KEY,
        Body=mock.ANY,
        ContentMD5="1B2M2Y8AsgTpgAmY7PhCfg==",
        ContentLength=0,
        Metadata={
            "exodus-migration-md5": "94e19d5d30b26306167e9e7bae6b28fd",
            "exodus-migration-src": "original/source",
            "gw-uploader": "user fake-user",
        },
    )

    # It should succeed
    assert r.status_code == 200

    # It should return the correct headers
    assert r.headers["etag"] == "a1b2c3"
    assert r.headers["content-length"] == "0"
    assert r.headers["x-request-id"]

    # It should have an empty body
    assert r.content == b""


async def test_part_upload(mock_aws_client, mock_request_reader, auth_header):
    """Uploading part of an object is delegated correctly to S3."""

    mock_request_reader.return_value = b"best bytes"
    mock_aws_client.upload_part.return_value = {"ETag": "a1b2c3"}
    mock_aws_client.head_object.side_effect = ClientError(
        {
            "Error": {
                "Code": "NoSuchKey",
                "Message": "The specified key does not exist.",
            },
            "ResponseMetadata": {"HTTPStatusCode": 404},
        },
        "GetObject",
    )

    with TestClient(app) as client:
        r = client.put(
            "/upload/test/%s?uploadId=my-upload&partNumber=88" % TEST_KEY,
            headers=auth_header(roles=["test-blob-uploader"]),
        )

    # It should succeed
    assert r.status_code == 200

    # It should return the correct headers
    assert r.headers["etag"] == "a1b2c3"
    assert r.headers["content-length"] == "0"
    assert r.headers["x-request-id"]

    # It should have an empty body
    assert r.content == b""


async def test_part_upload_duplicate(mock_aws_client, auth_header, caplog):
    """Calling multipart_put when the object has already been uploaded
    aborts the mpu and returns a valid response."""

    caplog.set_level(10, "s3")

    mock_aws_client.head_object.return_value = {
        "Key": TEST_KEY,
        "ETag": "a1b2c3",
        "Metadata": {},
    }

    with TestClient(app) as client:
        r = client.put(
            "/upload/test/%s?uploadId=my-upload&partNumber=3" % TEST_KEY,
            headers=auth_header(roles=["test-blob-uploader"]),
        )

    # It should check HEAD, log message, and issue abort
    mock_aws_client.head_object.assert_called()
    assert "s3 object already exists: %s" % TEST_KEY in caplog.text
    mock_aws_client.abort_multipart_upload.assert_called()
    # It should not upload
    mock_aws_client.upload_part.assert_not_called()
    # It should not try to complete upload
    mock_aws_client.complete_multipart_upload.assert_not_called()

    assert r.status_code == 200
    assert r.headers["etag"] == "a1b2c3"
    assert r.headers["content-length"] == "0"
    assert r.headers["x-request-id"]
    assert r.content == b""


@pytest.mark.parametrize(
    "metadata,err_msg",
    [
        ({"x-amz-meta-foo": "bar"}, "Invalid metadata field"),
        (
            {"x-amz-meta-exodus-migration-md5": "This is not a valid md5sum."},
            "Invalid value for metadata field",
        ),
    ],
    ids=["invalid field", "invalid value"],
)
async def test_upload_invalid_metadata(
    metadata,
    err_msg,
    mock_aws_client,
    mock_request_reader,
    auth_header,
    monkeypatch,
):
    """Uploading an object with invalid metadata raises an error"""

    monkeypatch.setenv(
        "EXODUS_GW_UPLOAD_META_FIELDS",
        '{"exodus-migration-md5": "^[0-9a-f]{32}$"}',
    )

    mock_request_reader.return_value = b"some bytes"
    mock_aws_client.put_object.return_value = {"ETag": "a1b2c3"}

    with TestClient(app) as client:
        r = client.put(
            "/upload/test/%s" % TEST_KEY,
            headers={**auth_header(roles=["test-blob-uploader"]), **metadata},
        )

    # It should fail with the correct error
    assert r.status_code == 400
    assert err_msg in r.text


async def test_put_error(mock_aws_client, mock_request_reader, auth_header):
    """An error response on an upload to S3 is passed back to the client correctly."""

    headers = auth_header(roles=["test-blob-uploader"])
    headers["X-Request-Id"] = "aabbccdd"

    mock_request_reader.return_value = b"some bytes"
    mock_aws_client.put_object.side_effect = ClientError(
        {
            "Error": {
                "Code": "SomeError",
                "Message": "A fake error for testing",
            },
            "ResponseMetadata": {
                "HTTPStatusCode": 412,
            },
        },
        "PutObject",
    )

    with TestClient(app) as client:
        r = client.put("/upload/test/%s" % TEST_KEY, headers=headers)

    # It should fail, with same status as in the boto error
    assert r.status_code == 412

    # The body should contain an S3-style XML error similar to:
    # https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html#RESTErrorResponses
    assert r.text == (
        "<?xml version='1.0' encoding='UTF-8'?>\n"
        "<Error><Code>SomeError</Code>"
        "<Message>A fake error for testing</Message>"
        "<Resource>/upload/test/b5bb9d8014a0f9b1d61e21e796d78dccdf1352f23cd32812f4850b878ae4944c</Resource>"
        "<RequestId>aabbccdd</RequestId>"
        "</Error>"
    )


async def test_already_uploaded_exc(mock_aws_client):
    mock_aws_client.head_object.side_effect = ClientError(
        {
            "Error": {
                "Code": "SomeUnknownError",
                "Message": "AWS S3 implemented some new error and we somehow triggered it",
            },
            "ResponseMetadata": {"HTTPStatusCode": 500},
        },
        "GetObject",
    )
    env = get_environment("test")
    settings = load_settings()

    request = mock.Mock()
    request.body = ""
    request.app.state.settings = settings
    request.app.state.s3_queues = {}

    s3_client = await get_s3_client(
        request=request, env=env, settings=settings
    ).__anext__()

    with pytest.raises(ClientError) as e:
        await _already_uploaded(env, s3_client, TEST_KEY)
        assert e.response["Error"]["Code"] == "SomeUnknownError"


async def test_ensure_aborted_exc(mock_aws_client):
    mock_aws_client.abort_multipart_upload.side_effect = ClientError(
        {
            "Error": {
                "Code": "SomeUnknownError",
                "Message": "AWS S3 implemented some new error and we somehow triggered it",
            },
            "ResponseMetadata": {"HTTPStatusCode": 500},
        },
        "GetObject",
    )
    env = get_environment("test")
    settings = load_settings()

    request = mock.Mock()
    request.body = ""
    request.app.state.settings = settings
    request.app.state.s3_queues = {}

    s3_client = await get_s3_client(
        request=request, env=env, settings=settings
    ).__anext__()

    with pytest.raises(ClientError) as e:
        await _ensure_aborted("my-upload", env, s3_client, TEST_KEY)
        assert e.response["Error"]["Code"] == "SomeUnknownError"
