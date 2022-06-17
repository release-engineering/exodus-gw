import asyncio
import logging
from datetime import datetime, timezone
from os.path import basename
from typing import List

import dramatiq
from dramatiq.middleware import CurrentMessage
from sqlalchemy import select
from sqlalchemy.orm import Session, lazyload

from exodus_gw.aws.dynamodb import write_batches
from exodus_gw.database import db_engine
from exodus_gw.models import Item, Publish, Task
from exodus_gw.schemas import PublishStates, TaskStates
from exodus_gw.settings import Settings

LOG = logging.getLogger("exodus-gw")

from .autoindex import AutoindexEnricher


class Commit:
    def __init__(
        self,
        publish_id: str,
        env: str,
        from_date: str,
        actor_msg_id: str,
        settings: Settings,
    ):
        self.env = env
        self.from_date = from_date
        self.rollback_item_ids: List[str] = []
        self.settings = settings
        self.db = Session(bind=db_engine(self.settings))
        self.task = self._query_task(actor_msg_id)
        self.publish = self._query_publish(publish_id)

    @property
    def task_ready(self) -> bool:
        task = self.task
        now = datetime.now(timezone.utc)
        if task.state in (TaskStates.complete, TaskStates.failed):
            LOG.warning(
                "Task %s in unexpected state, '%s'", task.id, task.state
            )
            return False
        if task.deadline and (task.deadline.timestamp() < now.timestamp()):
            LOG.warning("Task %s expired at %s", task.id, task.deadline)
            # Fail expired task and associated publish
            self.task.state = TaskStates.failed
            self.publish.state = PublishStates.failed
            self.db.commit()
            return False
        return True

    @property
    def publish_ready(self) -> bool:
        if self.publish.state == PublishStates.committing:
            return True
        LOG.warning(
            "Publish %s in unexpected state, '%s'",
            self.publish.id,
            self.publish.state,
        )
        return False

    @property
    def has_items(self) -> bool:
        item_count = (
            self.db.query(Item)
            .filter(Item.publish_id == self.publish.id)
            .count()
        )
        if item_count > 0:
            LOG.debug(
                "Prepared to write %d item(s) for publish %s",
                item_count,
                self.publish.id,
            )
            return True
        LOG.debug("No items to write for publish %s", self.publish.id)
        return False

    def _query_task(self, actor_msg_id: str) -> Task:
        return self.db.query(Task).filter(Task.id == actor_msg_id).first()

    def _query_publish(self, publish_id: str) -> Publish:
        publish = (
            self.db.query(Publish)
            .filter(Publish.id == publish_id)
            .options(lazyload(Publish.items))
            .first()
        )
        return publish

    def should_write(self) -> bool:
        if not self.task_ready:
            return False
        if not self.publish_ready:
            self.task.state = TaskStates.failed
            self.db.commit()
            return False
        if not self.has_items:
            self.task.state = TaskStates.complete
            self.publish.state = PublishStates.committed
            self.db.commit()
            return False
        return True

    def write_publish_items(self) -> None:
        """Query for publish items, batching and yielding them to
        conserve memory, and submit batch write requests."""

        # Save any entry point items to publish last.
        final_items: List[Item] = []

        statement = (
            select(Item)
            .where(Item.publish_id == self.publish.id)
            .execution_options(yield_per=self.settings.item_yield_size)
        )
        partitions = self.db.execute(statement).partitions()
        for partition in partitions:
            items: List[Item] = []

            # Flatten partition and extract any entry point items.
            for row in partition:
                item = row.Item
                if basename(item.web_uri) in self.settings.entry_point_files:
                    final_items.append(item)
                else:
                    items.append(item)

            # Save IDs of this chunk of items in case rollback is needed.
            self.rollback_item_ids.extend([item.id for item in items])
            # Submit write requests for this chunk of items.
            write_batches(self.env, items, self.from_date)

        if final_items:
            # Save entry point item IDs in case rollback is needed.
            self.rollback_item_ids.extend([item.id for item in final_items])
            # Submit write requests for entry point items.
            write_batches(self.env, final_items, self.from_date)

    def rollback_publish_items(self, exception: Exception) -> None:
        """Breaks the list of item IDs into chunks and iterates over
        each, querying corresponding items and submitting batch delete
        requests.
        """

        LOG.warning(
            "Rolling back %d item(s) due to error",
            len(self.rollback_item_ids),
            exc_info=exception,
        )

        chunk_size = self.settings.item_yield_size
        for index in range(0, len(self.rollback_item_ids), chunk_size):
            item_ids = self.rollback_item_ids[index : index + chunk_size]
            if item_ids:
                del_items = self.db.query(Item).filter(Item.id.in_(item_ids))
                write_batches(self.env, del_items, self.from_date, delete=True)

    def autoindex(self):
        enricher = AutoindexEnricher(self.publish, self.env, self.settings)
        asyncio.run(enricher.run())


@dramatiq.actor(time_limit=Settings().actor_time_limit)
def commit(
    publish_id: str, env: str, from_date: str, settings: Settings
) -> None:
    actor_msg_id = CurrentMessage.get_current_message().message_id
    commit_obj = Commit(publish_id, env, from_date, actor_msg_id, settings)

    if not commit_obj.should_write():
        return

    commit_obj.task.state = TaskStates.in_progress
    commit_obj.db.commit()

    # If any index files should be automatically generated for this publish,
    # generate and add them now.
    # No DynamoDB writes happen here, so this doesn't need to be covered by
    # the rollback handler.
    commit_obj.autoindex()

    try:
        commit_obj.write_publish_items()
        commit_obj.task.state = TaskStates.complete
        commit_obj.publish.state = PublishStates.committed
        commit_obj.db.commit()
    except Exception as exc_info:  # pylint: disable=broad-except
        LOG.exception("Task %s encountered an error", commit_obj.task.id)
        commit_obj.rollback_publish_items(exc_info)
        commit_obj.task.state = TaskStates.failed
        commit_obj.publish.state = PublishStates.failed
        commit_obj.db.commit()
        return
