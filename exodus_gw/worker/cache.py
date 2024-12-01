import logging
import os
import re
from datetime import datetime

import dramatiq
import fastpurge
from dramatiq.middleware import CurrentMessage
from sqlalchemy.orm import Session

from exodus_gw import models
from exodus_gw.aws.dynamodb import DynamoDB
from exodus_gw.aws.util import uris_with_aliases
from exodus_gw.database import db_engine
from exodus_gw.schemas import TaskStates
from exodus_gw.settings import Settings, get_environment

LOG = logging.getLogger("exodus-gw")


def exclude_path(path: str) -> bool:
    # Returns True for certain paths which should be excluded from cache flushing.
    if path.endswith("/treeinfo") and not path.endswith("/kickstart/treeinfo"):
        # RHELDST-24308: paths matching these conditions get a forced 404 response
        # without going to the CDN origin. This has the side-effect of breaking
        # cache flushing for those paths - if we request flush for these paths
        # we'll get an error.
        LOG.debug("Skipping %s: treeinfo fast-404 case", path)
        return True
    return False


class Flusher:
    def __init__(
        self,
        paths: list[str],
        settings: Settings,
        env: str,
        aliases: list[tuple[str, str, list[str]]],
    ):
        self.paths = [p for p in paths if not exclude_path(p)]
        self.settings = settings
        self.aliases = aliases

        for environment in settings.environments:
            if environment.name == env:
                self.env = environment

        assert self.env

    def arl_ttl(self, path: str):
        # Return an appropriate TTL value for certain paths.
        #
        # Note that this logic has to match the behavior configured at
        # the CDN edge.
        #
        # This logic was originally sourced from rhsm-akamai-cache-purge.

        ttl = "30d"  # default ttl
        ostree_re = r".*/ostree/repo/refs/heads/.*/(base|standard)$"
        if path.endswith(("/repodata/repomd.xml", "/")):
            ttl = "4h"
        elif (
            path.endswith(("/PULP_MANIFEST", "/listing"))
            or ("/repodata/" in path)
            or re.match(ostree_re, path)
        ):
            ttl = "10m"

        return ttl

    @property
    def urls_for_flush(self):
        out: list[str] = []

        # Get all paths for flush, resolving aliases.
        # Paths should have "/" removed because we're going to format
        # them with the CDN base URL and with ARL templates.
        path_list = [
            p.removeprefix("/")
            for p in uris_with_aliases(self.paths, self.aliases)
        ]

        for path in path_list:
            # Figure out the templates applicable to this path
            templates: list[str] = []
            for rule in self.env.cache_flush_rules:
                if rule.matches(path):
                    templates.extend(rule.templates)

            for template in templates:
                if "{path}" in template:
                    # interpret as a template with placeholders
                    out.append(
                        template.format(
                            path=path.removeprefix("/"),
                            ttl=self.arl_ttl(path),
                        )
                    )
                else:
                    # no {path} placeholder, interpret as a root URL
                    out.append(os.path.join(template, path))

        return out

    def do_flush(self, urls: list[str]):
        if not self.env.fastpurge_enabled or not urls:
            LOG.info("fastpurge is not enabled for %s", self.env.name)
            return

        for url in urls:
            LOG.info("fastpurge: flushing", extra=dict(url=url))

        fp = fastpurge.FastPurgeClient(
            auth=dict(
                host=self.env.fastpurge_host,
                access_token=self.env.fastpurge_access_token,
                client_token=self.env.fastpurge_client_token,
                client_secret=self.env.fastpurge_client_secret,
            )
        )

        responses = fp.purge_by_url(urls).result()

        for r in responses:
            LOG.info("fastpurge: response", extra=dict(response=r))

    def run(self):
        urls = self.urls_for_flush
        self.do_flush(urls)

        LOG.info(
            "%s flush of %s URL(s) (%s, ...)",
            "Completed" if self.env.fastpurge_enabled else "Skipped",
            len(urls),
            urls[0] if urls else "<empty>",
        )


def load_task(db: Session, task_id: str):
    return (
        db.query(models.Task)
        .filter(models.Task.id == task_id)
        .with_for_update()
        .first()
    )


@dramatiq.actor(
    time_limit=Settings().actor_time_limit,
    max_backoff=Settings().actor_max_backoff,
)
def flush_cdn_cache(
    paths: list[str],
    env: str,
    settings: Settings = Settings(),
) -> None:
    db = Session(bind=db_engine(settings))
    message = CurrentMessage.get_current_message()
    assert message
    task_id = message.message_id

    task = load_task(db, task_id)

    if task and task.state == TaskStates.not_started:
        # Mark the task in progress so clients know we're working on it...
        task.state = TaskStates.in_progress
        db.commit()

        # The commit dropped our "for update" lock, so reload it.
        task = load_task(db, task_id)

    if not task or task.state != TaskStates.in_progress:
        LOG.error(
            "Task in unexpected state %s", task.state if task else "<absent>"
        )
        return

    if task.deadline and task.deadline < datetime.utcnow():
        LOG.error("Task exceeded deadline of %s", task.deadline)
        task.state = TaskStates.failed
        db.commit()
        return

    # The CDN config is needed for alias resolution.
    ddb = DynamoDB(
        env=env,
        settings=settings,
        from_date=str(datetime.utcnow()),
        env_obj=get_environment(env, settings),
    )

    flusher = Flusher(
        paths=paths,
        settings=settings,
        env=env,
        aliases=ddb.aliases_for_flush,
    )
    flusher.run()

    task.state = TaskStates.complete
    db.commit()
