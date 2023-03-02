from datetime import datetime

from sqlalchemy import DateTime, String, event
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.types import Uuid

from .base import Base


class Task(Base):
    __tablename__ = "tasks"

    id: Mapped[str] = mapped_column(Uuid(as_uuid=False), primary_key=True)
    publish_id: Mapped[str] = mapped_column(Uuid(as_uuid=False))
    state: Mapped[str] = mapped_column(String, nullable=False)
    updated: Mapped[datetime] = mapped_column(DateTime())
    deadline: Mapped[datetime] = mapped_column(DateTime())


@event.listens_for(Task, "before_update")
def task_before_update(_mapper, _connection, task):
    task.updated = datetime.utcnow()
