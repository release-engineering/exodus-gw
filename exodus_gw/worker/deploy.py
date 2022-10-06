import json
import logging
from typing import Any, Dict

import dramatiq
from dramatiq.middleware import CurrentMessage
from sqlalchemy.orm import Session

from exodus_gw import models, schemas
from exodus_gw.aws.dynamodb import batch_write
from exodus_gw.database import db_engine
from exodus_gw.settings import Settings, get_environment

LOG = logging.getLogger("exodus-gw")


@dramatiq.actor(time_limit=Settings().actor_time_limit)
def complete_deploy_config_task(task_id: str):
    settings = Settings()
    db = Session(bind=db_engine(settings))
    task = db.query(models.Task).filter(models.Task.id == task_id).first()

    if task.state != "IN_PROGRESS":
        LOG.warning("Task %s in unexpected state, '%s'", task.id, task.state)
        return

    task.state = schemas.TaskStates.complete
    db.commit()

    LOG.info("Task %s completed successfully", task.id)


@dramatiq.actor(time_limit=Settings().actor_time_limit)
def deploy_config(config: Dict[str, Any], env: str, from_date: str):
    env_obj = get_environment(env)

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

    task.state = schemas.TaskStates.in_progress
    db.commit()

    try:
        request = {
            env_obj.config_table: [
                {
                    "PutRequest": {
                        "Item": {
                            "from_date": {"S": from_date},
                            "config_id": {"S": "exodus-config"},
                            "config": {"S": json.dumps(config)},
                        }
                    }
                },
            ]
        }
        LOG.info("Task %s writing config from %s", task.id, from_date)
        batch_write(env_obj, request)
    except Exception:  # pylint: disable=broad-except
        LOG.exception("Task %s encountered an error", task.id)

        task.state = schemas.TaskStates.failed
        db.commit()
        return

    # TTL must be sent in milliseconds but the setting is in minutes for
    # convenience and consistency with other components.
    ttl = settings.config_cache_ttl * 60000
    msg = complete_deploy_config_task.send_with_options(
        kwargs={"task_id": str(task.id)}, delay=ttl
    )
    LOG.debug(
        "Sent task %s for completion via message %s", task.id, msg.message_id
    )
