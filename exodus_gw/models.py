import uuid

from sqlalchemy import Column, ForeignKey, String
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship

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
    items = relationship("Item", back_populates="publish")


class Item(Base):

    __tablename__ = "items"

    id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        unique=True,
        nullable=False,
    )
    web_uri = Column(String, nullable=False)
    object_key = Column(String, nullable=False)
    from_date = Column(String, nullable=False)
    publish_id = Column(
        UUID(as_uuid=True), ForeignKey("publishes.id"), nullable=False
    )

    publish = relationship("Publish", back_populates="items")
