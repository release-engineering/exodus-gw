from datetime import datetime, timezone

from sqlalchemy import Column, DateTime, String, event
from sqlalchemy.dialects.postgresql import UUID

from .base import Base


class Task(Base):

    __tablename__ = "tasks"

    id = Column(UUID(as_uuid=True), primary_key=True)
    publish_id = Column(UUID(as_uuid=True))
    state = Column(String, nullable=False)
    updated = Column(DateTime(timezone=True))


@event.listens_for(Task, "before_update")
def task_before_update(_mapper, _connection, task):
    task.updated = datetime.now(timezone.utc)
