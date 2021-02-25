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
from datetime import datetime, timezone
from typing import Dict, List, Union
from uuid import UUID, uuid4

from fastapi import APIRouter, Body, HTTPException
from sqlalchemy.orm import Session

from .. import auth, deps, models, schemas, worker
from ..aws.util import validate_object_key
from ..settings import Environment, Settings

LOG = logging.getLogger("exodus-gw")

openapi_tag = {"name": "publish", "description": __doc__}

router = APIRouter(tags=[openapi_tag["name"]])


@router.post(
    "/{env}/publish",
    summary="Create new publish",
    response_model=schemas.Publish,
    status_code=200,
    responses={
        200: {
            "description": "Publish created",
            "content": {
                "application/json": {
                    "example": {
                        "id": "497f6eca-6276-4993-bfeb-53cbbbba6f08",
                        "env": "prod",
                        "links": {
                            "self": "/prod/publish/497f6eca-6276-4993-bfeb-53cbbbba6f08",
                            "commit": "/prod/publish/497f6eca-6276-4993-bfeb-53cbbbba6f08/commit",
                        },
                        "items": [],
                    }
                }
            },
        }
    },
    dependencies=[auth.needs_role("publisher")],
)
def publish(
    env: Environment = deps.env, db: Session = deps.db
) -> models.Publish:
    """Creates and returns a new publish object.

    **Required roles**: `{env}-publisher`
    """

    db_publish = models.Publish(id=uuid4(), env=env.name, state="PENDING")
    db.add(db_publish)

    return db_publish


@router.put(
    "/{env}/publish/{publish_id}",
    status_code=200,
    response_model=schemas.EmptyResponse,
    dependencies=[auth.needs_role("publisher")],
)
def update_publish_items(
    items: Union[schemas.ItemBase, List[schemas.ItemBase]] = Body(
        ...,
        example=[
            {
                "web_uri": "/my/awesome/file.iso",
                "object_key": "aec070645fe53ee3b3763059376134f058cc337247c978add178b6ccdfb0019f",
            },
            {
                "web_uri": "/my/slightly-less-awesome/other-file.iso",
                "object_key": "c06545d4e1a1c8e221d47e7d568c035fb32c6b6124881fd0bc17983bd9088ae0",
            },
        ],
    ),
    publish_id: UUID = schemas.PathPublishId,
    env: Environment = deps.env,
    db: Session = deps.db,
) -> Dict[None, None]:
    """Add publish items to an existing publish object.

    **Required roles**: `{env}-publisher`

    Publish items primarily are a mapping between a URI relative to the root of the CDN,
    and the key of a binary object which should be exposed from that URI.

    Adding items to a publish does not immediately make them available from the CDN;
    the publish object must first be committed.

    Items cannot be added to a publish once it has been committed.
    """

    db_publish = (
        db.query(models.Publish)
        .with_for_update()
        .filter(models.Publish.id == publish_id)
        .first()
    )

    if db_publish.state != "PENDING":
        raise HTTPException(
            status_code=409,
            detail="Publish %s in unexpected state, '%s'"
            % (db_publish.id, db_publish.state),
        )

    # Coerce single items to list.
    if not isinstance(items, list):
        items = [items]

    for item in items:
        validate_object_key(item.object_key)

        db.add(models.Item(**item.dict(), publish_id=db_publish.id))

    return {}


@router.post(
    "/{env}/publish/{publish_id}/commit",
    status_code=200,
    response_model=schemas.Task,
    dependencies=[auth.needs_role("publisher")],
)
def commit_publish(
    publish_id: UUID = schemas.PathPublishId,
    env: Environment = deps.env,
    db: Session = deps.db,
    settings: Settings = deps.settings,
) -> models.Task:
    """Commit an existing publish object.

    **Required roles**: `{env}-publisher`

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

    db_publish = (
        db.query(models.Publish)
        .with_for_update()
        .filter(models.Publish.id == publish_id)
        .first()
    )

    if db_publish.state != "PENDING":
        raise HTTPException(
            status_code=409,
            detail="Publish %s in unexpected state, '%s'"
            % (db_publish.id, db_publish.state),
        )

    msg = worker.commit.send(
        publish_id=str(db_publish.id),
        env=env.name,
        from_date=str(datetime.now(timezone.utc)),
    )

    LOG.info("Enqueued commit for '%s'", msg.kwargs["publish_id"])
    db_publish.state = schemas.PublishStates.committing

    task = models.Task(
        id=msg.message_id,
        publish_id=msg.kwargs["publish_id"],
        state="NOT_STARTED",
    )
    db.add(task)

    return task
