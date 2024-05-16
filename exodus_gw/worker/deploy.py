import logging
from typing import Any

import dramatiq
from dramatiq.middleware import CurrentMessage
from sqlalchemy.orm import Session

from exodus_gw import models, schemas
from exodus_gw.aws.dynamodb import DynamoDB
from exodus_gw.database import db_engine
from exodus_gw.settings import Settings

from .cache import Flusher

LOG = logging.getLogger("exodus-gw")


@dramatiq.actor(
    time_limit=Settings().actor_time_limit,
    max_backoff=Settings().actor_max_backoff,
)
def complete_deploy_config_task(
    task_id: str,
    settings: Settings = Settings(),
    flush_paths: list[str] | None = None,
    env: str | None = None,
):
    db = Session(bind=db_engine(settings))
    task = db.query(models.Task).filter(models.Task.id == task_id).first()

    assert task

    if task.state != "IN_PROGRESS":
        LOG.warning(
            "Task %s in unexpected state, '%s'",
            task.id,
            task.state,
            extra={"event": "deploy"},
        )
        return

    if env and flush_paths:
        flusher = Flusher(
            paths=flush_paths,
            settings=settings,
            env=env,
            # In this context Flusher does not need aliases, because
            # the flush_paths passed into us have already had alias
            # resolution applied.
            aliases=[],
        )
        flusher.run()

    task.state = schemas.TaskStates.complete
    db.commit()

    LOG.info(
        "Task %s completed successfully",
        task.id,
        extra={"event": "deploy", "success": True},
    )


def _listing_paths_for_flush(config: dict[str, Any]) -> set[str]:
    # extract listing paths from config that might have
    # updated values influencing the response of /listing
    # endpoint
    listing_paths: set[str] = set()

    for path in config.get("listing", {}).keys():
        lpath = path + "/listing"
        LOG.info(
            "Listing %s will flush cache for %s",
            path,
            lpath,
            extra={"event": "deploy"},
        )
        listing_paths.add(lpath)

    return listing_paths


@dramatiq.actor(
    time_limit=Settings().actor_time_limit,
    max_backoff=Settings().actor_max_backoff,
)
def deploy_config(
    config: dict[str, Any],
    env: str,
    from_date: str,
    settings: Settings = Settings(),
):
    db = Session(bind=db_engine(settings))
    ddb = DynamoDB(env, settings, from_date)

    original_aliases = {src: dest for (src, dest) in ddb.aliases_for_flush}

    current_message_id = CurrentMessage.get_current_message().message_id
    task = (
        db.query(models.Task)
        .filter(models.Task.id == current_message_id)
        .first()
    )

    assert task

    if task.state not in ("NOT_STARTED", "IN_PROGRESS"):
        LOG.warning(
            "Task %s in unexpected state, '%s'",
            task.id,
            task.state,
            extra={"event": "deploy"},
        )
        return

    task.state = schemas.TaskStates.in_progress
    db.commit()

    try:
        LOG.info(
            "Task %s writing config from %s",
            task.id,
            from_date,
            extra={"event": "deploy"},
        )
        ddb.write_config(config)

    except Exception:  # pylint: disable=broad-except
        LOG.exception(
            "Task %s encountered an error",
            task.id,
            extra={"event": "deploy", "success": False},
        )

        task.state = schemas.TaskStates.failed
        db.commit()
        return

    # After the write propagates, we may need to flush cache for some
    # URLs depending on what changed in the config.
    flush_paths: set[str] = set()

    for src, updated_dest in ddb.aliases_for_flush:
        if original_aliases.get(src) != updated_dest:
            for published_path in db.query(models.PublishedPath).filter(
                models.PublishedPath.env == env,
                models.PublishedPath.web_uri.like(f"{src}/%"),
            ):
                LOG.info(
                    "Updated alias %s will flush cache for %s",
                    src,
                    published_path.web_uri,
                    extra={"event": "deploy"},
                )
                flush_paths.add(published_path.web_uri)

    # Include all the listing paths for flush when enabled in settings
    flush_paths = (
        flush_paths.union(_listing_paths_for_flush(config))
        if settings.cdn_listing_flush
        else flush_paths
    )

    # TTL must be sent in milliseconds but the setting is in minutes for
    # convenience and consistency with other components.
    ttl = settings.config_cache_ttl * 60000
    msg = complete_deploy_config_task.send_with_options(
        kwargs={
            "task_id": str(task.id),
            "env": env,
            "flush_paths": sorted(flush_paths),
        },
        delay=ttl,
    )
    LOG.debug(
        "Sent task %s for completion via message %s",
        task.id,
        msg.message_id,
        extra={"event": "deploy"},
    )
