"""The exodus-gw service provides APIs for uploading and publishing content on the exodus CDN.

Available APIs are grouped into the following categories:

- [service](#tag/service): inspect the state of the exodus-gw service.
- [upload](#tag/upload): upload blobs to the CDN without exposing them to end-users.
  S3 compatible.
- [publish](#tag/publish): atomically publish a set of blobs on the CDN under specified paths,
  making them accessible to end-users.
- [deploy](#tag/deploy): deploy configuration influencing the behavior of the CDN.
- [cdn](#tag/cdn): utilities for accessing the CDN.

## Overview of API usage

A typical content publishing workflow using exodus-gw will consist of:

- Use the upload APIs to ensure desired blobs are uploaded.
   - As this API is partially S3-compatible, this can typically be done using
     an existing S3 client library.
- Use the publish API to create a publish object and create a (URI => blob)
  mapping for the blobs you want to publish.
- When you are ready to expose content to end-users, commit the publish object.
  This will atomically unveil new content at all of the requested URIs.


## Authentication

The exodus-gw API does not include any direct support for authentication and is
instead expected to be deployed behind a reverse-proxy implementing any desired
authentication mechanism.

If you are deploying an instance of exodus-gw, see
[the deployment guide](https://release-engineering.github.io/exodus-gw/deployment.html)
for information on how to integrate an authentication mechanism.

If you are a client looking to make use of exodus-gw, consult your organization's
internal documentation for advice on how to authenticate with exodus-gw.


## Environments

Many APIs in exodus-gw use the concept of an "environment" to control the target system
of an operation.

The set of environments is configured when exodus-gw is deployed.
A typical scenario is to deploy a "pre" environment for pre-release content and a
"live" environment for live content.

Different environments will also require the user to hold different roles. For example,
a client might be permitted only to write to one of the configured environments, or all
of them, depending on the configuration of the server.

If you are deploying an instance of exodus-gw, see
[the deployment guide](https://release-engineering.github.io/exodus-gw/deployment.html)
for information on how to configure environments.

If you are a client looking to make use of exodus-gw, consult your organization's
internal documentation for advice on which environment(s) you should be using.
"""

import logging
import re
from uuid import uuid4

import backoff
import botocore.exceptions
import dramatiq
from asgi_correlation_id import CorrelationIdMiddleware, correlation_id
from fastapi import Depends, FastAPI, Request
from fastapi.exception_handlers import http_exception_handler
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import Session
from starlette.concurrency import run_in_threadpool
from starlette.exceptions import HTTPException as StarletteHTTPException

from .auth import log_login
from .aws.util import xml_response
from .database import db_engine
from .logging import loggers_init
from .migrate import db_migrate
from .routers import cdn, deploy, publish, service, upload
from .settings import load_settings

app = FastAPI(
    title="exodus-gw",
    # This is the API version, which should follow SemVer rules.
    # The API version is not the same thing as the exodus-gw project version.
    version="1.0.0",
    description=__doc__,
    openapi_tags=[
        service.openapi_tag,
        upload.openapi_tag,
        publish.openapi_tag,
        deploy.openapi_tag,
        cdn.openapi_tag,
    ],
    dependencies=[Depends(log_login)],
)
app.include_router(service.router)
app.include_router(upload.router)
app.include_router(publish.router)
app.include_router(deploy.router)
app.include_router(cdn.router)

LOG = logging.getLogger("exodus-gw")


@app.exception_handler(Exception)
async def unhandled_exception_handler(request, exc):
    return await http_exception_handler(
        request,
        StarletteHTTPException(
            500,
            "Internal server error",
            headers={"X-Request-ID": correlation_id.get() or ""},
        ),
    )


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request, exc):
    # When validating requests, multiple violations are possible.
    # Let's return them all to expedite troubleshooting.
    msgs = [e["msg"] for e in exc.errors()]
    return JSONResponse(status_code=400, content={"detail": msgs})


@app.exception_handler(StarletteHTTPException)
async def custom_http_exception_handler(request, exc):
    # Override HTTPException to produce XML error responses for the
    # given endpoints.

    path = request.scope.get("path")

    if path.startswith("/upload"):
        return xml_response(
            "Error",
            status_code=exc.status_code,
            # Note in the real S3, the Code here is meant to be a short
            # string like "AccessDenied". But in this generic handler
            # we have no idea what that should be, hence the reusing of
            # the HTTP status code.
            #
            # In most cases an error should be surfacing in the ClientError
            # handler below instead, which handles things more accurately.
            Code=exc.status_code,
            Message=exc.detail,
            Endpoint=path,
        )

    return await http_exception_handler(request, exc)


@app.exception_handler(botocore.exceptions.ClientError)
async def boto_exception_handler(
    request, exc: botocore.exceptions.ClientError
):
    path = request.scope.get("path")

    # The /upload API is the only API where boto is (directly) used internally,
    # and so is the only API where we expect to reach this handler.
    #
    # It's also the only API where the currently implemented behavior makes sense,
    # as the XML responses are S3-specific. If we manage to get here outside
    # of "/upload", it's a bug.
    assert path.startswith("/upload")

    # In the case of the /upload API, we want it to be S3-compatible and to pass
    # on any errors from the real upstream S3. Extract the error detail from the
    # exception and return it as an S3-style XML response.
    #
    # This is important so that boto or other S3 clients using exodus-gw will get
    # the expected error handling behavior (e.g. it should retry on certain status
    # codes and not others).
    #
    # Example of the exc.response structure:
    #
    # {
    #     "Error": {
    #         "Code": "SomeServiceException",
    #         "Message": "Details/context around the exception or error",
    #     },
    #     "ResponseMetadata": {
    #         "RequestId": "1234567890ABCDEF",
    #         "HostId": "host ID data will appear here as a hash",
    #         "HTTPStatusCode": 400,
    #         "HTTPHeaders": {"header metadata key/values will appear here"},
    #         "RetryAttempts": 0,
    #     },
    # }
    #
    # Note, if any parsing of exc.response fails here we'll crash and
    # the generic 500 internal server error handler will take over.
    LOG.debug("Processing upload error from boto: %s", exc.response)

    return xml_response(
        "Error",
        status_code=exc.response["ResponseMetadata"]["HTTPStatusCode"],
        Code=exc.response["Error"]["Code"],
        Message=exc.response["Error"]["Message"],
        Resource=path,
        RequestId=correlation_id.get() or "",
    )


def db_init() -> None:
    app.state.db_engine = db_engine(app.state.settings)
    db_migrate(app.state.db_engine, app.state.settings)


def db_shutdown() -> None:
    app.state.db_engine.dispose()
    del app.state.db_engine


def settings_init() -> None:
    app.state.settings = load_settings()


def s3_queues_init() -> None:
    app.state.s3_queues = {}


async def s3_queues_shutdown() -> None:
    for q in app.state.s3_queues.values():
        while not q.empty():
            client = q.get_nowait()
            await client.__aexit__(None, None, None)


@app.on_event("startup")
def on_startup() -> None:
    settings_init()
    loggers_init(app.state.settings)
    db_init()
    s3_queues_init()


@app.on_event("shutdown")
async def on_shutdown() -> None:
    db_shutdown()
    await s3_queues_shutdown()


def new_db_session(engine):
    # Make a new DB session for use in the current request.
    #
    # This is in its own function so that it can be wrapped by tests.
    return Session(bind=engine, autoflush=False, autocommit=False)


@app.middleware("http")
def db_session(request: Request, call_next):
    """Maintain a DB session around each request, which is also shared
    with the dramatiq broker.

    An implicit commit occurs if and only if the request succeeds.
    """

    max_tries = app.state.settings.db_session_max_tries

    @backoff.on_exception(backoff.expo, DBAPIError, max_tries=max_tries)
    async def db_session_wrap():
        request.state.db = new_db_session(app.state.db_engine)

        # Any dramatiq operations should also make use of this session.
        broker = dramatiq.get_broker()
        broker.set_session(request.state.db)

        try:
            response = await call_next(request)
        except DBAPIError:
            await run_in_threadpool(request.state.db.rollback)
            raise
        else:
            if response.status_code >= 200 and response.status_code < 300:
                await run_in_threadpool(request.state.db.commit)
        finally:
            broker.set_session(None)
            await run_in_threadpool(request.state.db.close)
            request.state.db = None

        return response

    return db_session_wrap()


def request_id_validator(short_uuid: str):
    return re.match(r"^[0-9a-f]{8}$", short_uuid)


app.add_middleware(
    CorrelationIdMiddleware,
    generator=lambda: uuid4().hex[:8],
    validator=request_id_validator,
)
