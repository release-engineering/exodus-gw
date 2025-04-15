"""APIs for publishing blobs.

In the context of exodus-gw, "publishing" a blob means exposing it
via one or more user-accessible paths on the CDN. These blobs should first
be uploaded using the [upload](#tag/upload) APIs.


## Atomicity

exodus-gw aims to enable atomic semantics for publishes; i.e., for a set
of published content, committing the publish will make either *all* of it
available (if commit succeeds) or *none* of it available (if commit fails),
with no partial updates becoming visible from the point of view of a CDN
client.

In practice, limitations in the storage backend mean that true atomicity
is not achieved for all but the smallest publishes, so it is more accurate
to refer to the commit operation as "near-atomic".

Here are a few of the strategies used by exodus-gw in order to achieve as
close as possible to atomic behavior:

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
  See "two-phase commit" below for a more in-depth explanation of this.

## Two-phase commit

All published content is categorized into two phases, phase 1 and phase 2,
and committed in that order. exodus-gw performs this categorization internally
and clients cannot influence this.

Simple clients do not need to worry about this, but in more complicated scenarios
the client may wish to control the commit of each phase independently. In such
cases it is important to understand how the two phases are intended to work.

Phase 1 content:

- includes the majority of content within a publish
- should be immutable
- is usually not discoverable by CDN users without consulting some form of index
- examples: RPM files within a yum repo; any generic file

Phase 2 content:

- includes a small minority of content within a publish
- is usually mutable, perhaps changing at every publish
- contains indexes, repository entry points or other references pointing at
  phase 1 content (and thus must be committed last)
- examples: `repodata/repomd.xml` within a yum repo; `PULP_MANIFEST` within a
  Pulp file repository

As an example of this phased approach, consider the publish of a yum repository.
A client consuming packages from a yum repository discovers available packages
via a series of fetches involving multiple files which are published together,
e.g.

`repodata/repomd.xml` => `repodata/<checksum>-primary.xml.gz`
  => `Packages/<somepackage>.rpm`

If no ordering were to be applied to the publish of these files it would be
possible for `repomd.xml` to be published prior to `<checksum>-primary.xml.gz`,
or for `<checksum>-primary.xml.gz` to be published prior to
`Packages/<somepackage>.rpm`, either of which could cause a CDN consumer to
attempt to fetch content which has not yet been published, resulting in 404
errors.

This problem is avoided by exodus-gw internally categorizing `repomd.xml` as
phase 2 content and ensuring it is committed only after the rest of the files
in the repo, which are categorized as phase 1 content.

## Expiry of publish objects

Publish objects should be treated as ephemeral; they are not persisted indefinitely.

- All publish objects which have reached a terminal state (failed or committed) will be
  deleted after some server-defined timeout, defaulting to two weeks.
- Publish objects which have been created but not committed within a server-defined timeout,
  typically one day, will be marked as failed.


## Expiry of task objects

Like publish objects, task objects created when publishes are committed are not persisted
indefinitely.

- All task objects which have reached a terminal state (failed or complete) will be
  deleted after some server-defined timeout, defaulting to two weeks.
- Task objects not picked up by a worker within a server-defined time limit, defaulting
  to two hours, will be marked as failed along with the associated publish object. This
  prevents system overload in the event of a worker outage.
"""

import logging
import os
import re
from datetime import datetime
from uuid import uuid4

from fastapi import APIRouter, Body, HTTPException, Query
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session, noload

from .. import auth, deps, models, schemas, worker
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
                    "examples": [
                        {
                            "value": {
                                "id": "497f6eca-6276-4993-bfeb-53cbbbba6f08",
                                "env": "live",
                                "links": {
                                    "self": "/live/publish/497f6eca-6276-4993-bfeb-53cbbbba6f08",
                                    "commit": "/live/publish/497f6eca-6276-4993-bfeb-53cbbbba6f08/commit",
                                },
                                "items": [],
                            }
                        }
                    ]
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

    db_publish = models.Publish(id=str(uuid4()), env=env.name, state="PENDING")
    db.add(db_publish)

    return db_publish


@router.put(
    "/{env}/publish/{publish_id}",
    status_code=200,
    response_model=schemas.EmptyResponse,
    dependencies=[auth.needs_role("publisher")],
)
def update_publish_items(
    items: list[schemas.ItemBase] = Body(
        ...,
        examples=[
            [
                {
                    "web_uri": "/my/awesome/file.iso",
                    "object_key": "aec070645fe53ee3b3763059376134f058cc337247c978add178b6ccdfb0019f",
                    "content_type": "application/octet-stream",
                },
                {
                    "web_uri": "/my/slightly-less-awesome/other-file.iso",
                    "object_key": "c06545d4e1a1c8e221d47e7d568c035fb32c6b6124881fd0bc17983bd9088ae0",
                    "content_type": "application/octet-stream",
                },
                {
                    "web_uri": "/another/route/to/my/awesome/file.iso",
                    "link_to": "/my/awesome/file.iso",
                },
                {
                    "web_uri": "/my/awesome/deletion.iso",
                    "object_key": "absent",
                },
            ]
        ],
    ),
    publish_id: str = schemas.PathPublishId,
    env: Environment = deps.env,
    db: Session = deps.db,
    settings: Settings = deps.settings,
    call_context: auth.CallContext = deps.call_context,
    caller_roles: set[str] = deps.caller_roles,
) -> dict[None, None]:
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
        .with_for_update(read=True)
        .filter(
            models.Publish.id == publish_id,
            models.Publish.env == env.name,
        )
        .first()
    )

    if db_publish is None:
        raise HTTPException(
            status_code=404, detail="No publish found for ID %s" % publish_id
        )

    if db_publish.state != "PENDING":
        raise HTTPException(
            status_code=409,
            detail="Publish %s in unexpected state, '%s'"
            % (db_publish.id, db_publish.state),
        )

    # Resolve links before saving, as much as possible. The point of this is to
    # pay the cost of link resolution early rather than saving it all up for
    # commit.
    #
    # We want to resolve links on:
    #
    # (1) any existing items whose 'link_to' matches one of the 'web_uri' in
    #     a non-link item about to be saved, and...
    # (2) any item we're about to save whose 'link_to' matches one of the 'web_uri'
    #     of a non-link item already in the DB.
    #
    resolvable: list[schemas.ItemBase | models.Item] = []
    resolvable.extend(items)
    resolvable.extend(
        db.query(models.Item)
        # Items should be locked for update. However, as the point of this early
        # link resolution is to improve performance, if we would have to wait for
        # the lock on some items, just skip them instead and leave them to be
        # handled at commit time.
        .with_for_update(skip_locked=True)
        .filter(models.Item.publish_id == publish_id)
        .filter(
            models.Item.link_to.in_(
                [i.web_uri for i in items if not i.link_to]
            )
        )
        .order_by(models.Item.web_uri)
        .all()
    )

    db_publish.resolve_links(ln_items=resolvable)

    # Apply additional policy checks to the incoming items (e.g. do paths
    # abide by certain conventions). Users with a certain role are allowed
    # to bypass these checks.
    can_ignore_policy = f"{env.name}-ignore-policy" in caller_roles
    for item in items:
        try:
            item.validate_policy()
        except schemas.ItemPolicyError:
            if not can_ignore_policy:
                raise

    # Prevent unauthorized users from publishing to restricted paths within
    # a particular CDN environment.
    #
    # Some users only need to publish to certain paths. Allowing those
    # users to publish to other paths increases the risk of conflicts
    # between clients, or of accidents with a large impact.
    username = str(
        call_context.client.serviceAccountId
        or call_context.user.internalUsername
    )
    path_restrictions = (settings.publish_paths.get(env.name) or {}).get(
        username
    ) or [".*"]
    path_patterns = [re.compile(path) for path in path_restrictions]

    for i in items:
        # Determine whether the client is authorized to publish to this URI.
        authorized = False
        for pattern in path_patterns:
            if re.match(pattern, i.web_uri):
                authorized = True
                break
        if not authorized:
            # The URI did not match one of the client's permitted patterns in publish_paths.
            LOG.error(
                "User '%s' is not authorized to publish to path '%s'",
                username,
                i.web_uri,
                extra={
                    "publish_id": publish_id,
                    "event": "publish",
                    "success": False,
                },
            )

            raise HTTPException(
                403,
                detail="User '%s' is not authorized to publish to path '%s'"
                % (username, i.web_uri),
            )

    # Convert the list into dict and update each dict with a publish_id.
    # Each item's 'dirty' and 'updated' are refreshed to ensure it's
    # written to DynamoDB with the current update, even if it was already
    # written before.
    now = datetime.utcnow()
    items_data = [
        {
            **item.model_dump(),
            "publish_id": db_publish.id,
            "dirty": True,
            "updated": now,
        }
        for item in items
    ]
    items_data.sort(key=lambda item: item["web_uri"])

    LOG.debug(
        "Adding %s items into '%s'",
        len(items_data),
        db_publish.id,
        extra={"event": "publish"},
    )

    statement = insert(models.Item)

    # Update all target table columns, except for the primary_key column.
    update_dict = {c.name: c for c in statement.excluded if not c.primary_key}

    update_statement = statement.on_conflict_do_update(
        index_elements=["publish_id", "web_uri"],
        set_=update_dict,
    )

    db.execute(update_statement, items_data)

    # If any of the items we just updated are an entry point, we also trigger
    # autoindex in the background.
    #
    # Note that:
    #
    # - This is not a critical part of the publish handling. It's done to save
    #   time by having the indexes already prepared by the time of a later
    #   commit, but it doesn't matter if this is skipped or fails.
    #   A full autoindex always happens at commit time, catching anything not
    #   handled by this process.
    #
    # - No task is returned to the client. The client has no idea that this is
    #   happening, and no way to track the progress.
    #
    # - The way of queuing this actor is imperfect: just because the client has
    #   added an entry point file does not imply that they've also added all the
    #   files referenced by it. e.g. the client could have added repomd.xml
    #   without yet adding the *-primary.xml.gz referenced by it. In such cases,
    #   the autoindexing might fail. This is acknowledged. Per comments above,
    #   the failure is not critical - it just means the commit which happens
    #   later will have a bit more work to do.
    #
    entrypoint_paths = set()
    for item in items:
        if item.object_key == "absent":
            # deleted items don't get indexed
            continue

        basename = os.path.basename(item.web_uri)
        for entrypoint_basename in settings.entry_point_files:
            if basename == entrypoint_basename:
                if any(
                    exclude in item.web_uri
                    for exclude in settings.autoindex_partial_excludes
                ):
                    # Not eligible for partial autoindex, e.g. /kickstart/ repos
                    # because the kickstart and yum repodata might arrive separately.
                    LOG.info(
                        "%s: excluded from partial autoindex", item.web_uri
                    )
                else:
                    entrypoint_paths.add(item.web_uri)

    if entrypoint_paths:
        msg = worker.autoindex_partial.send(
            publish_id=publish_id,
            entrypoint_paths=sorted(entrypoint_paths),
        )

        LOG.info(
            "Enqueued autoindex on %s for paths: %s",
            msg.kwargs["publish_id"],
            ", ".join(sorted(entrypoint_paths)),
        )

    return {}


@router.post(
    "/{env}/publish/{publish_id}/commit",
    status_code=200,
    response_model=schemas.Task,
    dependencies=[auth.needs_role("publisher")],
)
def commit_publish(
    publish_id: str = schemas.PathPublishId,
    env: Environment = deps.env,
    db: Session = deps.db,
    settings: Settings = deps.settings,
    deadline: datetime = deps.deadline,
    commit_mode: models.CommitModes | None = Query(
        default=None,
        title="commit mode",
        description="See: [Two-phase commit](#section/Two-phase-commit)",
        examples=[models.CommitModes.phase1, models.CommitModes.phase2],
    ),
) -> models.CommitTask:
    """Commit an existing publish object.

    **Required roles**: `{env}-publisher`

    Committing a publish is required in order to expose published content from the CDN.

    There are two available commit modes, "phase1" and "phase2" (default).

    ### Phase 1

    A phase 1 commit:

    - is optional.
    - can be performed more than once.
    - does not prevent further modifications to the publish.
    - will commit all phase 1 content (e.g. packages in yum repos), but not phase 2
      content (e.g. repodata in yum repos); see
      [Two-phase commit](#section/Two-phase-commit).
    - is not rolled back if a later phase 2 commit fails (or never occurs).

    ### Phase 2

    A phase 2 commit:

    - is the default when no commit mode is specified.
    - can (and should) be performed exactly once.
    - freezes the associated publish object - no further items can be added.
    - will commit all content with near-atomic behavior; see
      [Atomicity](#section/Atomicity).

    ### Notes

    Commit occurs asynchronously.  This API returns a Task object which may be used
    to monitor the progress of the commit.

    Note that exodus-gw does not require that any given path is only modified by
    a single publish. If multiple publish objects updating the same path are being
    committed at the same time, after both commits succeed, the path will point
    at whichever item was updated most recently.
    """
    commit_mode_str = (commit_mode or models.CommitModes.phase2).value
    now = datetime.utcnow()

    db_publish = (
        db.query(models.Publish)
        # Publish should be locked, but if doing a phase1 commit we will only
        # be reading from the publish and not writing to it.
        .with_for_update(read=commit_mode_str == models.CommitModes.phase1)
        .filter(
            models.Publish.id == publish_id,
            models.Publish.env == env.name,
        )
        .first()
    )

    if not db_publish:
        raise HTTPException(
            status_code=404, detail="No publish found for ID %s" % publish_id
        )

    if db_publish.state != "PENDING":
        if commit_mode_str == models.CommitModes.phase2:
            # Phase 2 commit can only be done once, so asking to commit again is
            # an error, but to make the API idempotent we check if there is
            # already an associated task and return it if so.
            task = (
                db.query(models.CommitTask)
                .filter(
                    models.CommitTask.publish_id == publish_id,
                    models.CommitTask.commit_mode == commit_mode_str,
                )
                .first()
            )
            if task:
                return task

        raise HTTPException(
            status_code=409,
            detail="Publish %s in unexpected state, '%s'"
            % (db_publish.id, db_publish.state),
        )

    db_publish.resolve_links()

    msg = worker.commit.send(
        publish_id=str(db_publish.id),
        env=env.name,
        from_date=str(now),
        commit_mode=commit_mode_str,
    )

    LOG.info(
        "Enqueued %s commit for '%s'",
        commit_mode_str,
        msg.kwargs["publish_id"],
        extra={"event": "publish", "success": True},
    )

    # Only phase2 commit moves the publish into committing state.
    if commit_mode_str == models.CommitModes.phase2:
        db_publish.state = schemas.PublishStates.committing

    task = models.CommitTask(
        id=msg.message_id,
        publish_id=msg.kwargs["publish_id"],
        state="NOT_STARTED",
        deadline=deadline,
        commit_mode=commit_mode,
    )
    db.add(task)

    return task


@router.get(
    "/{env}/publish/{publish_id}",
    response_model=schemas.Publish,
    status_code=200,
    responses={
        200: {
            "description": "Publish found",
            "content": {
                "application/json": {
                    "examples": [
                        {
                            "value": {
                                "id": "497f6eca-6276-4993-bfeb-53cbbbba6f08",
                                "env": "live",
                                "links": {
                                    "self": "/live/publish/497f6eca-6276-4993-bfeb-53cbbbba6f08",
                                    "commit": "/live/publish/497f6eca-6276-4993-bfeb-53cbbbba6f08/commit",
                                },
                                "items": [],
                            }
                        }
                    ]
                }
            },
        },
        404: {
            "description": "Publish not found",
            "content": "No publish found for ID 497f6eca-6276-4993-bfeb-53cbbbba6f08",
        },
    },
    dependencies=[auth.needs_role("publisher")],
)
async def get_publish(
    publish_id: str = schemas.PathPublishId,
    env: Environment = deps.env,
    db: Session = deps.db,
):
    """Return an existing publish object from database using the given publish ID.

    For performance reasons, the returned item list is always empty.
    """

    db_publish = (
        db.query(models.Publish)
        .options(noload(models.Publish.items))
        .filter(
            models.Publish.id == publish_id,
            models.Publish.env == env.name,
        )
        .first()
    )

    if not db_publish:
        raise HTTPException(
            status_code=404, detail="No publish found for ID %s" % publish_id
        )

    return db_publish
