"""APIs for publishing blobs.

In the context of exodus-gw, "publishing" a blob means exposing it
via one or more user-accessible paths on the CDN. These blobs should first
be uploaded using the [upload](#tag/upload) APIs.


## Atomicity

exodus-gw aims to deliver atomic semantics for publishes; i.e., for a set
of published content, committing the publish will make either *all* of it
available (if commit succeeds) or *none* of it available (if commit fails),
with no partial updates becoming visible from the point of view of a CDN
client.

In practice, limitations in the storage backend mean that true atomicity
is not achieved for all but the smallest publishes, so it is more accurate
to refer to the commit operation as "near-atomic".

Here are a few of the strategies used by exodus-gw in order to ensure the
atomicity of commit:

- exodus-gw performs writes to the underlying database (DynamoDB) in batches,
  which themselves are committed atomically.

  - (True atomicity could be achieved if the entire publish fits within
     a single batch, but as batches have a maximum size of 25 items, this
     is rarely the case.)

- All operations are aggressively retried in case of error.

- exodus-gw keeps track of what has been committed and is able to roll
  back a partially committed publish in case of unrecoverable errors.

- During commit, the items to be committed are prioritized intelligently
  with knowledge of the types of content being published. Files which serve
  as an index or entry point to a set of content are committed last, to ensure
  minimal impact in the case that a commit is interrupted.

  - Example: if a publish includes yum repositories, exodus-gw will ensure that
    repomd.xml files are always committed last - ensuring there is no possibility
    that an interrupted commit would unveil a repomd.xml file referencing other
    files which were not yet committed.

It should be noted that the atomicity discussed here applies only to the interaction
between exodus-gw and its underlying data store. exodus-gw does not contain any CDN
cache purging logic; the impact of CDN caching must also be considered when evaluating
the semantics of a publish from the CDN client's point of view.


## Expiry of publish objects

Publish objects should be treated as ephemeral; they are not persisted indefinitely.

- All publish objects which have reached a terminal state (failed or committed) will be
  deleted after some server-defined timeout, typically one week.
- Publish objects which have been created but not committed within a server-defined timeout,
  typically one day, will be marked as failed.
"""

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

openapi_tag = {"name": "publish", "description": __doc__}

router = APIRouter(tags=[openapi_tag["name"]])


@router.post(
    "/{env}/publish",
    summary="Create new publish",
    response_model=schemas.Publish,
    status_code=200,
)
async def publish(env: str, db: Session = Depends(get_db)) -> models.Publish:
    """Creates and returns a new publish object."""

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
    """Add publish items to an existing publish object.

    Publish items primarily are a mapping between a URI relative to the root of the CDN,
    and the key of a binary object which should be exposed from that URI.

    Adding items to a publish does not immediately make them available from the CDN;
    the publish object must first be committed.

    Items cannot be added to a publish once it has been committed.
    """

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
    """Commit an existing publish object.

    Committing a publish has the following effects:

    - All URIs contained within the publish become accessible from the CDN,
      pointing at their corresponding objects.
      - This occurs with all-or-nothing semantics; see [Atomicity](#section/Atomicity).
    - The publish object becomes frozen - no further items can be added.

    Commit occurs asynchronously.  This API returns a Task object which may be used
    to monitor the progress of the commit.

    Note that exodus-gw does not resolve conflicts or ensure that any given path is
    only modified by a single publish. If multiple publish objects covering the same
    path are being committed concurrently, URIs on the CDN may end up pointing to
    objects from any of those publishes.
    """

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
