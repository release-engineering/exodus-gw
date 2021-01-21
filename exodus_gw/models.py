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

    @property
    def aws_fmt(self):
        return {
            "web_uri": {"S": self.web_uri},
            "object_key": {"S": self.object_key},
            "from_date": {"S": self.from_date},
        }
