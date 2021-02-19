import logging
from typing import List, Union
from uuid import uuid4

from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Session

from . import models, schemas
from .aws.util import validate_object_key
from .settings import Environment

LOG = logging.getLogger("exodus-gw")


def create_publish(env: Environment, db: Session) -> models.Publish:
    db_publish = models.Publish(id=uuid4(), env=env.name, state="PENDING")
    db.add(db_publish)
    db.commit()
    db.refresh(db_publish)
    return db_publish


def update_publish(
    db: Session,
    items: Union[schemas.ItemBase, List[schemas.ItemBase]],
    publish_id: UUID,
) -> None:

    # Coerce single items to list.
    if not isinstance(items, list):
        items = [items]

    for item in items:
        validate_object_key(item.object_key)

        db.add(models.Item(**item.dict(), publish_id=publish_id))

    db.commit()
