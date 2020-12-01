from sqlalchemy.orm import Session

from .models import Publish


def create_publish(db: Session):
    db_publish = Publish()
    db.add(db_publish)
    db.commit()
    db.refresh(db_publish)
    return db_publish
