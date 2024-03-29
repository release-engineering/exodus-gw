import os

import aioboto3
import boto3.session
from botocore.config import Config

from .log import add_loggers


def boto_session(*args, **kwargs) -> boto3.session.Session:
    out = boto3.session.Session(*args, **kwargs)
    add_loggers(out)
    return out


def aioboto_session(*args, **kwargs) -> aioboto3.Session:
    out = aioboto3.Session(*args, **kwargs)
    add_loggers(out)
    return out


class S3ClientWrapper:
    """Helper class to obtain preconfigured S3 clients.

    Clients may be wrapped with additional config and event handlers.
    """

    def __init__(self, profile: str):
        """Prepare a client for the given profile. This object must be used
        via 'async with' in order to obtain access to the client.

        Note: Session creation will fail if provided profile cannot be found.
        """

        session = aioboto_session(profile_name=profile)

        self._client_context = session.client(
            "s3",
            endpoint_url=os.environ.get("EXODUS_GW_S3_ENDPOINT_URL") or None,
            # We don't allow any retries - it's not possible since we're streaming
            # request bodies directly to S3, we don't buffer it anywhere, so we
            # can't send it more than once.
            config=Config(retries={"total_max_attempts": 1}),
        )

    async def __aenter__(self):
        client = await self._client_context.__aenter__()

        client.meta.events.register(
            "needs-retry.s3.PutObject", self.no_redirects
        )
        client.meta.events.register(
            "needs-retry.s3.CreateMultipartUpload", self.no_redirects
        )

        return client

    async def __aexit__(self, exc_type, exc, tb):
        await self._client_context.__aexit__(exc_type, exc, tb)

    @staticmethod
    def no_redirects(**kwargs):
        # An event handler for needs-retry.s3.* events which will disable implicit
        # redirects between regions.
        #
        # The S3 client has a built-in feature where, if you do a request to a
        # bucket in the incorrect region, the client will parse the error response
        # from the server to detect the correct region and then try again.
        #
        # For example, if your config file points at us-east-2 region but your bucket
        # is at us-east-1, each request will first be issued to us-east-2 which will
        # fail, then will be issued again to us-east-1.
        #
        # We don't want this for two reasons:
        #
        # 1. If exodus-gw config has the target region incorrectly configured, we'd like
        #    to know about that immediately. This implicit redirect behavior (even if it
        #    worked) would cause most single-request operations to require two requests,
        #    which is bad for performance, so it would be better to detect and fix the
        #    incorrect config.
        #
        # 2. It's not possible to work anyway, because we are streaming requests from
        #    the caller directly to S3 without buffering them - meaning that the request
        #    body is not available anywhere to issue the same request again to a different
        #    region.
        #
        # Thus we disable this redirection feature here. This should be registered onto
        # relevant needs-retry.s3.* events before using any s3 client.
        #
        # Note that there doesn't seem to be any documented API for opting-out of this
        # boto behavior. The logic here depends on implementation details of the
        # S3RegionRedirector class in botocore/utils.py.
        request_dict = kwargs.get("request_dict") or {}
        context = request_dict.get("context") or {}
        context["s3_redirected"] = True
        # A newer version, S3RegionRedirectorv2, uses a different key. Let's add it too
        # since we can't determine or control which version is used.
        context["redirected"] = True


class DynamoDBClientWrapper:
    """Helper class to obtain preconfigured DynamoDB clients.

    Clients may be wrapped with additional config and event handlers.
    """

    def __init__(self, profile: str):
        """Prepare a client for the given profile.
        Note: Session creation will fail if provided profile cannot be found.
        """

        session = boto_session(profile_name=profile)

        self._client = session.client(
            "dynamodb",
            endpoint_url=os.environ.get("EXODUS_GW_DYNAMODB_ENDPOINT_URL")
            or None,
        )

    @property
    def client(self):
        return self._client
