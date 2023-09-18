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

The set of environments is configured when exodus-gw is deployed. For example, separate
"production" and "staging" environments may be configured, making use of separate storage
backends.

Different environments will also require the user to hold different roles. For example,
a client might be permitted only to write to one of the configured environments, or all
of them, depending on the configuration of the server.

If you are deploying an instance of exodus-gw, see
[the deployment guide](https://release-engineering.github.io/exodus-gw/deployment.html)
for information on how to configure environments.

If you are a client looking to make use of exodus-gw, consult your organization's
internal documentation for advice on which environment(s) you should be using.
"""

from uuid import uuid4

import backoff
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

app.add_middleware(CorrelationIdMiddleware, generator=lambda: uuid4().hex[:8])


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
            "Error", Code=exc.status_code, Message=exc.detail, Endpoint=path
        )

    return await http_exception_handler(request, exc)


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
