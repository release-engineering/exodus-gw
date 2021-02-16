import logging
from os.path import basename

import dramatiq
from dramatiq.middleware import CurrentMessage
from sqlalchemy.orm import Session

from exodus_gw import models, schemas
from exodus_gw.aws.dynamodb import write_batches
from exodus_gw.crud import get_publish_by_id
from exodus_gw.database import db_engine
from exodus_gw.settings import Settings

LOG = logging.getLogger("exodus-gw")


@dramatiq.actor(time_limit=Settings().actor_time_limit)
def commit(publish_id: str, env: str):
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

    items = []
    last_items = []

    for item in get_publish_by_id(db, publish_id).items:
        if basename(item.web_uri) in settings.entry_point_files:
            last_items.append(item)
        else:
            items.append(item)

    items_written = False
    last_items_written = False

    # Change task state to IN_PROGRESS.
    task.state = schemas.TaskStates.in_progress
    db.commit()

    try:
        if items:
            items_written = write_batches(env, items)

        if items_written and last_items:
            last_items_written = write_batches(env, last_items)

        if not items_written or (last_items and not last_items_written):
            items = items + last_items if last_items else items
            write_batches(env, items, delete=True)
            # Change task state to FAILED.
            task.state = schemas.TaskStates.failed
            db.commit()
            return
    except Exception:
        LOG.exception("Task %s encountered an error", task.id)
        # Change task state to FAILED.
        task.state = schemas.TaskStates.failed
        db.commit()
        return

    # Change task state to COMPLETE.
    task.state = schemas.TaskStates.complete
    db.commit()
