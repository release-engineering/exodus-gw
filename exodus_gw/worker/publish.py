import asyncio
import contextvars
import logging
from datetime import datetime
from os.path import basename
from queue import Empty, Full, Queue
from threading import Thread
from typing import Any, List, Optional

import dramatiq
from dramatiq.middleware import CurrentMessage
from sqlalchemy import select
from sqlalchemy.orm import Session, lazyload

from exodus_gw.aws.dynamodb import DynamoDB
from exodus_gw.database import db_engine
from exodus_gw.models import CommitModes, CommitTask, Item, Publish
from exodus_gw.schemas import PublishStates, TaskStates
from exodus_gw.settings import Settings, get_environment

from .autoindex import AutoindexEnricher
from .progress import ProgressLogger

LOG = logging.getLogger("exodus-gw")


class _BatchWriter:
    """Use as context manager recommended. Otherwise, the threads must
    be cleaned up manually.
    """

    def __init__(
        self,
        dynamodb: DynamoDB,
        settings: Settings,
        item_count: int,
        message: str,
        delete: bool = False,
    ):
        self.dynamodb = dynamodb
        self.settings = settings
        self.delete = delete
        self.queue: Any = Queue(self.settings.write_queue_size)
        self.sentinel = object()
        self.threads: List[Thread] = []
        self.errors: List[Exception] = []
        self.progress_logger = ProgressLogger(
            message=message,
            items_total=item_count,
        )

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

    def adjust_total(self, increment: int):
        self.progress_logger.adjust_total(increment)

    def start(self):
        for i in range(self.settings.write_max_workers):
            # These threads are considered as belonging to whatever actor spawned them.
            # This is indicated by propagating the context downwards.
            # Mainly influences logging.
            context = contextvars.copy_context()

            thread = Thread(
                name=f"batchwriter-{i}",
                daemon=True,
                target=context.run,
                args=(self.write_batches,),
            )
            thread.start()
            self.threads.append(thread)

    def stop(self):
        for _ in range(len(self.threads)):
            # A sentinel for each worker to get from the shared queue.
            try:
                self.queue.put(
                    self.sentinel,
                    timeout=self.settings.write_queue_timeout,
                )
            except Full as err:
                self.append_error(err)

        for thread in self.threads:
            thread.join()

        if self.queue.qsize() > 0:
            # Don't warn for excess sentinels.
            if not self.queue.get_nowait() is self.sentinel:
                self.append_error(
                    RuntimeError("Commit incomplete, queue not empty")
                )

        if self.errors:
            raise self.errors[0]

    def append_error(self, err: Exception):
        LOG.error(
            "Exception while submitting batch write(s)",
            exc_info=err,
            extra={"event": "publish", "success": False},
        )
        self.errors.append(err)

    def queue_batches(self, items: List[Item]) -> List[str]:
        batches = self.dynamodb.get_batches(items)
        timeout = self.settings.write_queue_timeout
        queued_item_ids: List[str] = []

        for batch in batches:
            # Don't attempt to put more items on the queue if error(s)
            # already encountered.
            if not self.errors:
                try:
                    self.queue.put(batch, timeout=timeout)
                    queued_item_ids.extend(
                        [str(item.id) for item in list(batch)]
                    )
                except Full as err:
                    self.append_error(err)

        return queued_item_ids

    def write_batches(self):
        """Will either submit batch write or delete requests based on
        the 'delete' attribute.
        """
        while not self.errors:
            # Don't attempt to write more batches if error(s) already
            # encountered by other thread(s).
            try:
                got = self.queue.get(timeout=self.settings.write_queue_timeout)
                if got is self.sentinel:
                    break
                self.dynamodb.write_batch(got, delete=self.delete)
                self.progress_logger.update(len(got))
            except (RuntimeError, ValueError, Empty) as err:
                if err is not Empty:
                    self.append_error(err)
                break


class CommitBase:
    # Acceptable publish states for this type of commit.
    # Subclasses need to override.
    PUBLISH_STATES: list[PublishStates] = []

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
        self.written_item_ids: List[str] = []
        self.settings = settings
        self.db = Session(bind=db_engine(self.settings))
        self.task = self._query_task(actor_msg_id)
        self.publish = self._query_publish(publish_id)
        self.env_obj = get_environment(env)
        self._dynamodb = None

    @property
    def dynamodb(self):
        if self._dynamodb is None:
            self._dynamodb = DynamoDB(
                self.env,
                self.settings,
                self.from_date,
                self.env_obj,
                self.task.deadline,
            )
        return self._dynamodb

    @property
    def task_ready(self) -> bool:
        task = self.task
        now = datetime.utcnow()
        if task.state in (TaskStates.complete, TaskStates.failed):
            LOG.warning(
                "Task %s in unexpected state, '%s'",
                task.id,
                task.state,
                extra={"event": "publish"},
            )
            return False
        if task.deadline and (task.deadline.timestamp() < now.timestamp()):
            LOG.warning(
                "Task %s expired at %s",
                task.id,
                task.deadline,
                extra={"event": "publish", "success": False},
            )
            self.on_failed()
            self.db.commit()
            return False
        return True

    @property
    def publish_ready(self) -> bool:
        if self.publish.state in self.PUBLISH_STATES:
            return True
        LOG.warning(
            "Publish %s in unexpected state, '%s'",
            self.publish.id,
            self.publish.state,
            extra={"event": "publish"},
        )
        return False

    @property
    def item_count(self):
        # Items included in publish.
        #
        # Intentionally not cached because the item count can be
        # changed during commit (e.g. autoindex)
        return (
            self.db.query(Item)
            .filter(Item.publish_id == self.publish.id)
            .count()
        )

    @property
    def has_items(self) -> bool:
        if self.item_count > 0:
            LOG.debug(
                "Prepared to write %d item(s) for publish %s",
                self.item_count,
                self.publish.id,
                extra={"event": "publish"},
            )
            return True
        LOG.debug(
            "No items to write for publish %s",
            self.publish.id,
            extra={"event": "publish", "success": True},
        )
        return False

    def _query_task(self, actor_msg_id: str):
        return (
            self.db.query(CommitTask)
            .filter(CommitTask.id == actor_msg_id)
            .first()
        )

    def _query_publish(self, publish_id: str):
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
            # An empty commit just instantly succeeds...
            self.on_succeeded()
            self.db.commit()
            return False
        return True

    @property
    def written_item_ids_batched(self):
        """self.written_item_ids, but batched into chunks."""
        chunk_size = self.settings.item_yield_size
        for index in range(0, len(self.written_item_ids), chunk_size):
            batch = self.written_item_ids[index : index + chunk_size]
            if batch:
                yield batch

    def write_publish_items(self) -> list[Item]:
        """Query for publish items, batching and yielding them to
        conserve memory, and submit batch write requests via
        _BatchWriter.

        The implementation on the base class handles phase1 items only
        and returns the list of uncommitted phase2 items.
        Subclasses should override this.
        """

        statement = (
            select(Item)
            .where(Item.publish_id == self.publish.id, Item.dirty == True)
            .with_for_update()
            .execution_options(yield_per=self.settings.item_yield_size)
        )
        partitions = self.db.execute(statement).partitions()

        # Save any entry point items to publish last.
        final_items: List[Item] = []

        wrote_count = 0

        # The queue is empty at this point but we want to write batches
        # as they're put rather than wait until they're all queued.
        with _BatchWriter(
            self.dynamodb,
            self.settings,
            self.item_count,
            "Writing phase 1 items",
        ) as bw:
            # Being queuing item batches.
            for partition in partitions:
                items: List[Item] = []

                # Flatten partition and extract any entry point items.
                for row in partition:
                    item: Item = row.Item
                    if (
                        basename(item.web_uri)
                        in self.settings.entry_point_files
                    ):
                        LOG.debug(
                            "Delayed write for %s",
                            item.web_uri,
                            extra={"event": "publish"},
                        )
                        final_items.append(item)
                        bw.adjust_total(-1)
                    else:
                        items.append(item)

                wrote_count += len(items)

                # Submit items to be batched and queued, saving item
                # IDs for rollback and marking as no longer dirty.
                self.written_item_ids.extend(bw.queue_batches(items))

        return final_items

    def rollback_publish_items(self, exception: Exception) -> None:
        """Breaks the list of item IDs into chunks and iterates over
        each, querying corresponding items and submitting batch delete
        requests.
        """

        LOG.warning(
            "Rolling back %d item(s) due to error",
            len(self.written_item_ids),
            exc_info=exception,
            extra={"event": "publish"},
        )

        for item_ids in self.written_item_ids_batched:
            with _BatchWriter(
                self.dynamodb,
                self.settings,
                len(item_ids),
                "Rolling back",
                delete=True,
            ) as bw:
                items = self.db.query(Item).filter(Item.id.in_(item_ids))
                bw.queue_batches(items)

    def on_succeeded(self):
        # Called when commit operation has succeeded.
        # The task has succeeded...
        self.task.state = TaskStates.complete

        # And any written items are no longer dirty.
        # We know they can't have been updated while we were running
        # because we selected them "FOR UPDATE" earlier.
        for item_ids in self.written_item_ids_batched:
            self.db.query(Item).filter(Item.id.in_(item_ids)).update(
                {Item.dirty: False}
            )

    def on_failed(self):
        # Called when commit operation has failed.
        self.task.state = TaskStates.failed

    def pre_write(self):
        # Any steps prior to DynamoDB write.
        # Base implementation does nothing.
        pass


class CommitPhase1(CommitBase):
    # phase1 commit is allowed to proceed in either of these states.
    PUBLISH_STATES = [PublishStates.committing, PublishStates.pending]

    def write_publish_items(self) -> list[Item]:
        final_items = super().write_publish_items()

        # In phase1 we don't process the final items, but we'll log
        # how many have been left for later.
        LOG.info(
            "Phase 1: committed %s items, phase 2: %s items remaining",
            len(self.written_item_ids),
            len(final_items),
            extra={"event": "publish"},
        )

        return []


class CommitPhase2(CommitBase):
    PUBLISH_STATES = [PublishStates.committing]

    def write_publish_items(self) -> list[Item]:
        final_items = super().write_publish_items()

        # In phase2 we go ahead and write the final items.
        LOG.info(
            "Phase 1: committed %s items, phase 2: committing %s items",
            len(self.written_item_ids),
            len(final_items),
            extra={"event": "publish"},
        )

        # Start a new context manager to raise any errors from previous
        # and skip additional write attempts.
        with _BatchWriter(
            self.dynamodb,
            self.settings,
            len(final_items),
            "Writing phase 2 items",
        ) as bw:
            if final_items:
                self.written_item_ids.extend(bw.queue_batches(final_items))

        return []

    def pre_write(self):
        # If any index files should be automatically generated for this publish,
        # generate and add them now before processing the items.
        enricher = AutoindexEnricher(self.publish, self.env, self.settings)
        asyncio.run(enricher.run())

    # phase2 commit also completes the publish, one way or another.
    def on_succeeded(self):
        super().on_succeeded()
        self.publish.state = PublishStates.committed

    def on_failed(self):
        super().on_failed()
        self.publish.state = PublishStates.failed


@dramatiq.actor(
    time_limit=Settings().actor_time_limit,
    max_backoff=Settings().actor_max_backoff,
)
def commit(
    publish_id: str,
    env: str,
    from_date: str,
    commit_mode: Optional[str] = None,
    settings: Settings = Settings(),
) -> None:
    actor_msg_id = CurrentMessage.get_current_message().message_id

    commit_mode = commit_mode or CommitModes.phase2.value
    commit_class: type[CommitBase] = {
        CommitModes.phase1.value: CommitPhase1,
        CommitModes.phase2.value: CommitPhase2,
    }[commit_mode]
    commit_obj = commit_class(
        publish_id, env, from_date, actor_msg_id, settings
    )

    if not commit_obj.should_write():
        return

    commit_obj.task.state = TaskStates.in_progress
    commit_obj.db.commit()

    # Do any relevant commit steps prior to the main DynamoDB writes.
    # Anything which happens here is not covered by rollback.
    commit_obj.pre_write()

    try:
        commit_obj.write_publish_items()
        commit_obj.on_succeeded()
        commit_obj.db.commit()
    except Exception as exc_info:  # pylint: disable=broad-except
        LOG.exception(
            "Task %s encountered an error",
            commit_obj.task.id,
            extra={"event": "publish", "success": False},
        )
        try:
            commit_obj.rollback_publish_items(exc_info)
        finally:
            commit_obj.on_failed()
            commit_obj.db.commit()
        return
