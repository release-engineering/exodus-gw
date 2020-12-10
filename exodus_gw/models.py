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

    def __init__(
        self,
        web_uri=web_uri,
        object_key=object_key,
        from_date=from_date,
        publish_id=publish_id,
    ):
        self.web_uri = web_uri
        self.object_key = object_key
        self.from_date = from_date
        self.publish_id = publish_id
