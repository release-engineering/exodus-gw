import aioboto3
import pytest

from exodus_gw.aws.client import S3ClientWrapper as s3_client


async def test_client_retries_disabled():
    """Verify that created S3 clients have retries disabled in config."""

    async with s3_client("test-profile-123"):
        # It should have obtained the client via a session,
        # passing a config object with retries disabled
        client_call = aioboto3.Session().client.mock_calls[0]
        config = client_call.kwargs["config"]
        assert config.retries == {"total_max_attempts": 1}


async def test_client_redirects_disabled():
    """Verify that created S3 clients have disabled the implicit region redirect feature
    in boto library."""

    async with s3_client("test-profile-123") as client:
        # It should have registered handlers for the relevant events
        register_calls = client.meta.events.register.mock_calls

        # Should have registered for these events
        event_names = [call.args[0] for call in register_calls]
        assert event_names == [
            "needs-retry.s3.PutObject",
            "needs-retry.s3.CreateMultipartUpload",
        ]

        # Should have registered the same handler for each one
        handlers = [call.args[1] for call in register_calls]
        handlers = list(set(handlers))
        assert len(handlers) == 1

        # And the handler should have the behavior of disabling
        # redirects by marking requests as already redirected
        request_dict = {"some": "fields", "context": {"foo": "bar"}}
        handlers[0](request_dict=request_dict)
        assert request_dict == {
            "some": "fields",
            "context": {
                "foo": "bar",
                "s3_redirected": True,
                "redirected": True,
            },
        }
