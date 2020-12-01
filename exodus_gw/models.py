import uuid

from sqlalchemy import Column
from sqlalchemy.dialects.postgresql import UUID

from .database import Base


class Publish(Base):

    __tablename__ = "publishes"

    id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        unique=True,
        nullable=False,
    )

    def __init__(self):
        pass
