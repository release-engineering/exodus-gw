import logging
from itertools import islice
from typing import List, Union
from uuid import UUID

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from .. import models, schemas, worker
from ..auth import CallContext, call_context
from ..aws.dynamodb import batch_write
from ..crud import create_publish, get_publish_by_id, update_publish
from ..database import SessionLocal
from ..settings import get_environment, get_settings

LOG = logging.getLogger("exodus-gw")

router = APIRouter()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@router.get("/healthcheck", tags=["service"])
def healthcheck():
    """Returns a successful response if the service is running."""
    return {"detail": "exodus-gw is running"}


@router.get("/healthcheck-worker", tags=["service"])
def healthcheck_worker():
    """Returns a successful response if background workers are running."""

    msg = worker.ping.send()

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
    """Returns a new, empty publish object"""

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
    """Update the publish objects with items"""

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
    """Write the publish's items, in batches, to DynamoDB table"""

    # Validate environment from caller.
    get_environment(env)

    publish_obj = get_publish_by_id(db, publish_id)
    items_iter = iter(publish_obj.items)
    batch_size = get_settings().batch_size
    batches = list(iter(lambda: tuple(islice(items_iter, batch_size)), ()))

    write_failed = False
    for batch in batches:
        response = await batch_write(env, batch)

        if response["UnprocessedItems"]:
            write_failed = True
            break

    if write_failed:
        LOG.info("One or more writes were unsuccessful")
        LOG.info("Cleaning up written items. . .")

        unprocessed_items = []
        for batch in batches:
            response = await batch_write(env, batch, delete=True)

            if response["UnprocessedItems"]:
                unprocessed_items.append(response["UnprocessedItems"])

        if unprocessed_items:
            LOG.error(
                "Unprocessed items:\n\t%s",
                ("\n\t".join([str(item) for item in unprocessed_items])),
            )
            raise RuntimeError(
                "Cleanup failed: partial publish persists!\n"
                "See error log for details"
            )

        LOG.info("Publish erased.")

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
