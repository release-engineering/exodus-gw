import logging
import re
from typing import Any

import dramatiq
from dramatiq.middleware import CurrentMessage
from sqlalchemy.orm import Session

from exodus_gw import models, schemas
from exodus_gw.aws.dynamodb import DynamoDB
from exodus_gw.aws.util import uris_with_aliases
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

    original_aliases = {src: dest for (src, dest, _) in ddb.aliases_for_flush}
    original_exclusions = {src: exc for (src, _, exc) in ddb.aliases_for_flush}

    message = CurrentMessage.get_current_message()
    assert message
    current_message_id = message.message_id
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

    updated_prefixes = set()
    for src, updated_dest, _ in ddb.aliases_for_config_update:
        if original_aliases.get(src) != updated_dest:
            updated_prefixes.add(src)

    # We need to use the bidirectional aliases here to resolve transitive
    # aliases.
    aliases_to_expand = [
        alias
        for alias in ddb.aliases_for_flush
        if alias[0] in original_aliases.keys()
        and alias[0] not in updated_prefixes
    ]

    updated_aliases = {src: dest for (src, dest, _) in ddb.aliases_for_flush}

    updated_prefixes.update(
        uris_with_aliases(updated_prefixes, aliases_to_expand)
    )

    for src in updated_prefixes:
        # Flush cache for content published on the src side of the alias
        for published_path in db.query(models.PublishedPath).filter(
            models.PublishedPath.env == env,
            models.PublishedPath.web_uri.like(f"{src}/%"),
        ):
            # If any original exclusion matches the uri, the uri wouldn't
            # have been treated as an alias, thus cache flushing would be
            # unnecessary.
            if any(
                re.search(exclusion, published_path.web_uri)
                for exclusion in original_exclusions.get(src, [])
            ):
                continue
            LOG.info(
                "Updated alias %s will flush cache for %s",
                src,
                published_path.web_uri,
                extra={"event": "deploy"},
            )
            flush_paths.add(published_path.web_uri)

        # Separately, check for additional content that was only published
        # on the destination side of the alias (e.g., kickstarts), and flush
        # the corresponding src path.
        #
        # If the content at the src side of the alias has already been flushed,
        # it will not be flushed twice, because flush_paths is a set.
        resolved_alias = updated_aliases.get(src)
        if resolved_alias:
            for published_path in db.query(models.PublishedPath).filter(
                models.PublishedPath.env == env,
                models.PublishedPath.web_uri.like(f"{resolved_alias}/%"),
            ):
                # If any original exclusion matches the uri, the uri wouldn't
                # have been treated as an alias, thus cache flushing would be
                # unnecessary.
                if any(
                    re.search(exclusion, published_path.web_uri)
                    for exclusion in original_exclusions.get(src, [])
                ):
                    continue

                resolved_uri = published_path.web_uri.replace(
                    resolved_alias, src
                )

                LOG.info(
                    "Updated alias %s will flush cache for %s",
                    src,
                    resolved_uri,
                    extra={"event": "deploy"},
                )
                flush_paths.add(resolved_uri)

    # Need to expand the matched aliases to other aliases that'd be populated
    # during publish
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
