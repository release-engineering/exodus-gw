"""AWS logging utilities."""

import logging
from typing import Any

import aioboto3
import boto3.session
from botocore.awsrequest import AWSPreparedRequest, AWSResponse

REQUEST_LOG = logging.getLogger("exodus-gw.aws-request")
RESPONSE_LOG = logging.getLogger("exodus-gw.aws-response")


def request_logger(request: AWSPreparedRequest, **_kwargs):
    # Callback for logging requests being sent to AWS.
    REQUEST_LOG.info(
        "%s %s",
        request.method,
        request.url,
        extra={
            "event": {
                "method": request.method,
                "url": request.url,
            }
        },
    )


def response_logger(
    response: tuple[AWSResponse, Any] | None,
    request_dict: dict[str, Any],
    caught_exception: Exception | None,
    **_kwargs
):
    # Callback for logging responses from AWS.
    url = response[0].url if response else request_dict["url"]
    event = {
        "method": request_dict["method"],
        "url": url,
    }
    summary = "<unknown result>"

    if caught_exception:
        summary = event["exception"] = repr(caught_exception)
    elif response:
        event["status"] = response[0].status_code
        summary = str(event["status"])

    RESPONSE_LOG.info(
        "%s %s: %s",
        request_dict["method"],
        url,
        summary,
        extra={"event": event},
    )


def add_loggers(session: boto3.session.Session | aioboto3.Session):
    """Add some custom loggers onto a boto session."""

    # Log just before we send requests.
    session.events.register("before-send.*", request_logger)

    # It's not entirely obvious, but needs-retry is actually the
    # best event for logging responses (rather than e.g. after-call).
    # It is the only event which gets access to both the request and
    # the response, and is also called both on success and failure.
    session.events.register("needs-retry.*", response_logger)
