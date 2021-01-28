import logging
from os.path import basename
from typing import List, Union
from uuid import UUID

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from .. import models, schemas
from ..aws.dynamodb import write_batches
from ..crud import create_publish, get_publish_by_id, update_publish
from ..database import get_db
from ..settings import get_environment, get_settings

LOG = logging.getLogger("exodus-gw")

router = APIRouter(tags=["publish"])


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
