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
    db: Session, items: List[schemas.ItemBase], publish_id: UUID
) -> None:
    db_items = []
    for item in items:
        db_item = models.Item(**item.dict(), publish_id=publish_id)
        db.add(db_item)
        db_items.append(db_item)
    db.commit()
    for db_item in db_items:
        db.refresh(db_item)
