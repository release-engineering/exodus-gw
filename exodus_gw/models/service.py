from datetime import datetime
from enum import Enum

from sqlalchemy import DateTime, ForeignKey, String, event
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.types import Uuid

from .base import Base


class CommitModes(str, Enum):
    phase1 = "phase1"
    phase2 = "phase2"


class Task(Base):
    __tablename__ = "tasks"
    __mapper_args__ = {
        "polymorphic_identity": "task",
        "polymorphic_on": "type",
    }

    id: Mapped[str] = mapped_column(Uuid(as_uuid=False), primary_key=True)
    type: Mapped[str]
    state: Mapped[str] = mapped_column(String)
    updated: Mapped[datetime | None] = mapped_column(DateTime())
    deadline: Mapped[datetime | None] = mapped_column(DateTime())


class CommitTask(Task):
    __tablename__ = "commit_tasks"
    __mapper_args__ = {
        "polymorphic_identity": "commit",
    }

    id: Mapped[str] = mapped_column(ForeignKey("tasks.id"), primary_key=True)
    publish_id: Mapped[str] = mapped_column(Uuid(as_uuid=False))
    commit_mode: Mapped[str] = mapped_column(
        String, default=CommitModes.phase2
    )


@event.listens_for(Task, "before_update")
@event.listens_for(CommitTask, "before_update")
def task_before_update(_mapper, _connection, task: Task):
    task.updated = datetime.utcnow()
