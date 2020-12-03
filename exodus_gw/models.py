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

    def __init__(self):
        pass


class Item(Base):

    __tablename__ = "items"

    uri = Column(
        String,
        primary_key=True,
        nullable=False,
    )
    object_key = Column(String, nullable=False)
    publish_id = Column(
        UUID(as_uuid=True), ForeignKey("publishes.id"), nullable=False
    )

    publish = relationship("Publish", back_populates="items")

    def __init__(self, uri=uri, object_key=object_key, publish_id=publish_id):
        self.uri = uri
        self.object_key = object_key
        self.publish_id = publish_id
