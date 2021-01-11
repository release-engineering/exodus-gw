from typing import List

from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Session, lazyload

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


def get_publish_by_id(db: Session, publish_id: UUID):
    return (
        db.query(models.Publish)
        .filter(models.Publish.id == publish_id)
        .options(lazyload(models.Publish.items))
        .first()
    )
