from typing import List

from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Session

from . import models, schemas


def create_publish(db: Session) -> List[models.Publish]:
    db_publish = models.Publish()
    db.add(db_publish)
    db.commit()
    db.refresh(db_publish)
    return db_publish


def update_publish(
    db: Session,
    items: schemas.ItemBase or List[schemas.ItemBase],
    publish_id: UUID,
) -> None:

    # Coerce single items to list.
    if not isinstance(items, list):
        items = [items]

    for item in items:
        db.add(models.Item(**item.dict(), publish_id=publish_id))

    db.commit()
