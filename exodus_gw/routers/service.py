"""APIs for inspecting the state of the exodus-gw service."""

import logging
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Header, HTTPException, Response
from sqlalchemy.orm import Session

from .. import deps, models, schemas
from ..auth import CallContext
from ..models import DramatiqConsumer
from ..settings import Settings

LOG = logging.getLogger("exodus-gw")

openapi_tag = {"name": "service", "description": __doc__}

router = APIRouter(tags=[openapi_tag["name"]])


@router.get(
    "/",
    include_in_schema=False,
)
async def redirect(accept: str | None = Header(default=None)):
    """Redirect from service root to API docs. For browsers only."""

    # We only send the redirect if it seems like the client is a browser.
    # The point is that this is not a part of the API and we want this to
    # be more or less hidden to your typical clients like curl, requests.
    if accept and "text/html" in accept:
        return Response(
            content=None, headers={"location": "/redoc"}, status_code=302
        )

    raise HTTPException(404)


@router.get(
    "/healthcheck",
    response_model=schemas.MessageResponse,
    responses={200: {"description": "Service is up"}},
)
async def healthcheck():
    """Returns a successful response if the service is running."""
    return {"detail": "exodus-gw is running"}


@router.get(
    "/healthcheck-worker",
    response_model=schemas.MessageResponse,
    responses={200: {"description": "Worker(s) are responding"}},
)
def healthcheck_worker(
    db: Session = deps.db, settings: Settings = deps.settings
):
    """Returns a successful response if background workers are running."""

    # consumer is alive if it was last seen at least this recently.
    threshold = datetime.now(timezone.utc) - timedelta(
        seconds=settings.worker_keepalive_timeout
    )

    alive_consumers = (
        db.query(DramatiqConsumer)
        .filter(DramatiqConsumer.last_alive >= threshold)
        .count()
    )

    if not alive_consumers:
        raise HTTPException(500, detail="background workers unavailable")

    return {"detail": "background worker is running"}


@router.get(
    "/whoami",
    response_model=CallContext,
    responses={
        200: {
            "description": "Caller's auth context retrieved",
            "content": {
                "application/json": {
                    "examples": [
                        {
                            "value": {
                                "client": {
                                    "roles": ["someRole", "anotherRole"],
                                    "authenticated": True,
                                    "serviceAccountId": "clientappname",
                                },
                                "user": {
                                    "roles": ["viewer"],
                                    "authenticated": True,
                                    "internalUsername": "someuser",
                                },
                            }
                        }
                    ]
                }
            },
        }
    },
)
async def whoami(context: CallContext = deps.call_context):
    """Return basic information on the caller's authentication & authorization context.

    This endpoint may be used to determine whether the caller is authenticated to
    the exodus-gw service, and if so, which set of role(s) are held by the caller.

    It is a read-only endpoint intended for diagnosing authentication issues.
    """
    return context


@router.get(
    "/task/{task_id}",
    response_model=schemas.Task,
    responses={200: {"description": "Sucessfully retrieved task"}},
)
def get_task(task_id: str = schemas.PathTaskId, db: Session = deps.db):
    """Return existing task object from database using given task ID."""
    task = db.query(models.Task).filter(models.Task.id == task_id).first()

    if not task:
        raise HTTPException(404, detail="No task found for ID '%s'" % task_id)

    return task
