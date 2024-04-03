import io
from dataclasses import dataclass

import boto3.session
import botocore
import pytest
from boto3.session import Session
from botocore.awsrequest import AWSPreparedRequest, AWSResponse

from exodus_gw.aws.client import boto_session


class RawResponse(io.BytesIO):
    # An object of the (undocumented?) type used by
    # AWSResponse to hold HTTP response body.
    def stream(self, **_kwargs):
        while contents := self.read():
            yield contents


@dataclass
class AWSResponder:
    # A callable to respond to AWS requests, if installed as a
    # before-send event handler.
    status_code: int = 200
    body: bytes = b""
    exception: Exception | None = None

    def __call__(self, request: AWSPreparedRequest, **_kwargs):
        if self.exception:
            raise self.exception

        return AWSResponse(
            url=request.url,
            status_code=self.status_code,
            headers={},
            raw=RawResponse(self.body),
        )


def test_client_logs_requests(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
):
    """Boto clients log AWS requests and responses."""

    # boto3.session.Session is usually mocked by an autouse fixture.
    # That's good, as we want to ensure all tests by default have no
    # chance of accidentally using real AWS.
    #
    # But the mock does not trigger realistic boto events, so is
    # incapable of realistic testing of the logging behavior.
    #
    # This test therefore patches back in the original Session
    # so we can construct a *real* client, though we will install
    # custom event handlers on it to prevent it doing any real
    # requests.
    monkeypatch.setattr(boto3.session, "Session", Session)

    session = boto_session(
        aws_access_key_id="fake",
        aws_secret_access_key="fake",
        aws_session_token="fake",
        region_name="fake",
    )

    s3 = session.client("s3")

    # Install our own object to generate fake responses before sending them.
    # This is an event handler, installed *after* the logging event handler,
    # so the logging handler will see requests as if they happened for real.
    responder = AWSResponder()
    s3.meta.events.register_last("before-send.*", responder)

    # Successful case:
    obj_url = "https://test-bucket.s3.fake.amazonaws.com/test-key"
    s3.head_object(Bucket="test-bucket", Key="test-key")

    assert caplog.messages == [
        f"HEAD {obj_url}",
        f"HEAD {obj_url}: 200",
    ]
    caplog.clear()

    # Graceful error case (still a valid HTTP response):
    responder.status_code = 501
    with pytest.raises(botocore.exceptions.ClientError):
        s3.head_object(Bucket="test-bucket", Key="test-key")

    assert caplog.messages == [
        f"HEAD {obj_url}",
        f"HEAD {obj_url}: 501",
    ]
    caplog.clear()

    # Ungraceful error case (no HTTP response):
    responder.exception = RuntimeError("simulated error")
    with pytest.raises(RuntimeError):
        s3.create_multipart_upload(Bucket="test-bucket", Key="test-key")

    assert caplog.messages == [
        f"POST {obj_url}?uploads",
        f"POST {obj_url}?uploads: RuntimeError('simulated error')",
    ]
