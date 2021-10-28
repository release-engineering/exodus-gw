import logging
from os.path import basename

import dramatiq
from dramatiq.middleware import CurrentMessage
from sqlalchemy.orm import Session, lazyload

from exodus_gw import models, schemas
from exodus_gw.aws.dynamodb import write_batches
from exodus_gw.database import db_engine
from exodus_gw.settings import Settings

LOG = logging.getLogger("exodus-gw")


@dramatiq.actor(time_limit=Settings().actor_time_limit)
def commit(publish_id: str, env: str, from_date: str):
    settings = Settings()
    db = Session(bind=db_engine(settings))
    current_message_id = CurrentMessage.get_current_message().message_id
    task = (
        db.query(models.Task)
        .filter(models.Task.id == current_message_id)
        .first()
    )

    if task.state not in ("NOT_STARTED", "IN_PROGRESS"):
        LOG.warning("Task %s in unexpected state, '%s'", task.id, task.state)
        return

    publish = (
        db.query(models.Publish)
        .filter(models.Publish.id == publish_id)
        .options(lazyload(models.Publish.items))
        .first()
    )

    if publish.state != "COMMITTING":
        LOG.warning(
            "Publish %s in unexpected state, '%s'", publish.id, publish.state
        )
        task.state = schemas.PublishStates.failed
        db.commit()
        return

    if publish.items:
        items = []
        last_items = []

        for item in publish.items:
            if basename(item.web_uri) in settings.entry_point_files:
                last_items.append(item)
            else:
                items.append(item)

        items_written = False
        last_items_written = False

        task.state = schemas.TaskStates.in_progress
        db.commit()

        try:
            if items:
                items_written = write_batches(env, items, from_date)

            if items_written and last_items:
                last_items_written = write_batches(env, last_items, from_date)

            if not items_written or (last_items and not last_items_written):
                items = items + last_items if last_items else items
                write_batches(env, items, from_date, delete=True)

                task.state = schemas.TaskStates.failed
                publish.state = schemas.PublishStates.failed
                db.commit()
                return
        except Exception:
            LOG.exception("Task %s encountered an error", task.id)

            task.state = schemas.TaskStates.failed
            publish.state = schemas.PublishStates.failed
            db.commit()
            return

    task.state = schemas.TaskStates.complete
    publish.state = schemas.PublishStates.committed
    db.commit()
