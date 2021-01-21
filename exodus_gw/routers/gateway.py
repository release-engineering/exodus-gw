import logging
from os.path import basename
from typing import List, Union
from uuid import UUID

from fastapi import APIRouter, Depends, Request
from sqlalchemy.orm import Session

from .. import models, schemas, worker
from ..auth import CallContext, call_context
from ..aws.dynamodb import write_batches
from ..crud import create_publish, get_publish_by_id, update_publish
from ..settings import get_environment, get_settings

LOG = logging.getLogger("exodus-gw")

router = APIRouter()


def get_db(request: Request):
    return request.state.db


@router.get("/healthcheck", tags=["service"])
def healthcheck():
    """Returns a successful response if the service is running."""
    return {"detail": "exodus-gw is running"}


@router.get("/healthcheck-worker", tags=["service"])
def healthcheck_worker(db: Session = Depends(get_db)):
    """Returns a successful response if background workers are running."""

    msg = worker.ping.send()

    # Message would not normally be sent until commit after the request succeeds.
    # Since we want to get the result, we'll commit early.
    db.commit()

    # If we don't get a response in time, this will raise an exception and we'll
    # respond with a 500 error, which seems reasonable.
    result = msg.get_result(block=True, timeout=5000)

    return {"detail": "background worker is running: ping => %s" % result}


@router.post(
    "/{env}/publish",
    response_model=schemas.Publish,
    status_code=200,
    tags=["publish"],
)
async def publish(env: str, db: Session = Depends(get_db)) -> models.Publish:
    """Returns a new, empty publish object."""

    # Validate environment from caller.
    get_environment(env)

    return create_publish(db)


@router.put(
    "/{env}/publish/{publish_id}",
    status_code=200,
    tags=["publish"],
)
async def update_publish_items(
    env: str,
    publish_id: UUID,
    items: Union[schemas.ItemBase or List[schemas.ItemBase]],
    db: Session = Depends(get_db),
) -> dict:
    """Update the publish objects with items."""

    # Validate environment from caller.
    get_environment(env)

    update_publish(db, items, publish_id)

    return {}


@router.post(
    "/{env}/publish/{publish_id}/commit",
    status_code=200,
    tags=["publish"],
)
async def commit_publish(
    env: str, publish_id: UUID, db: Session = Depends(get_db)
) -> dict:
    """Write the publish's items, in batches, to DynamoDB table."""

    # Validate environment from caller.
    get_environment(env)

    settings = get_settings()

    items = []
    items_written = False
    last_items = []
    last_items_written = False

    for item in get_publish_by_id(db, publish_id).items:
        if basename(item.web_uri) in settings.entry_point_files:
            last_items.append(item)
        else:
            items.append(item)

    if items:
        items_written = await write_batches(env, items)

    if not items_written:
        # Delete all items if failed to write any items.
        await write_batches(env, items, delete=True)
    elif last_items:
        # Write any last_items if successfully wrote all items.
        last_items_written = await write_batches(env, last_items)

        if not last_items_written:
            # Delete everything if failed to write last_items.
            await write_batches(env, items + last_items, delete=True)

    return {}


@router.get(
    "/whoami",
    response_model=CallContext,
    tags=["service"],
    responses={
        200: {
            "description": "returns caller's auth context",
            "content": {
                "application/json": {
                    "example": {
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
            },
        }
    },
)
def whoami(context: CallContext = Depends(call_context)):
    """Return basic information on the caller's authentication & authorization context.

    This endpoint may be used to determine whether the caller is authenticated to
    the exodus-gw service, and if so, which set of role(s) are held by the caller.

    It is a read-only endpoint intended for diagnosing authentication issues.
    """
    return context
