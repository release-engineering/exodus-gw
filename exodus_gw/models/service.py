from sqlalchemy import Column, String
from sqlalchemy.dialects.postgresql import UUID

from .base import Base


class Task(Base):

    __tablename__ = "tasks"

    id = Column(UUID(as_uuid=True), primary_key=True)
    publish_id = Column(UUID(as_uuid=True), nullable=False)
    state = Column(String, nullable=False)
