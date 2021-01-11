from typing import List
from uuid import UUID
from itertools import islice

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from .. import models, schemas
from ..auth import CallContext, call_context
from ..aws.dynamodb import batch_write
from ..crud import create_publish, update_publish, get_publish_by_id
from ..database import SessionLocal
from ..settings import get_environment

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


@router.post(
    "/{env}/publish",
    response_model=schemas.Publish,
    status_code=200,
    tags=["service"],
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
    items: schemas.ItemBase or List[schemas.ItemBase],
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
async def commit_publish(env: str, publish_id: UUID) -> dict:
    """Write the publish's items, in chunks of <= 25, to DynamoDB table"""

    publish = get_publish_by_id(publish_id)
    items_iter = iter(publish.items)

    for chunk in list(iter(lambda: tuple(islice(items_iter, 25)), ())):
        # Enqueue write for each chunk of 25 (or fewer) items.
        batch_write.send(env, list(chunk))

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
