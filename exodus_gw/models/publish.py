import uuid
from datetime import datetime

from fastapi import HTTPException
from sqlalchemy import (
    Column,
    DateTime,
    ForeignKey,
    String,
    event,
    func,
    inspect,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Bundle, relationship

from .base import Base


class Publish(Base):
    __tablename__ = "publishes"

    id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    env = Column(String, nullable=False)
    state = Column(String, nullable=False)
    updated = Column(DateTime())
    items = relationship(
        "Item", back_populates="publish", cascade="all, delete-orphan"
    )

    def resolve_links(self):
        db = inspect(self).session
        # Store only publish items with link targets.
        ln_items = (
            db.query(Item)
            .filter(Item.publish_id == self.id)
            .filter(func.coalesce(Item.link_to, "") != "")
            .all()
        )
        # Collect link targets of linked items for finding matches.
        ln_item_paths = [item.link_to for item in ln_items]

        # Store only necessary fields from matching items to conserve memory.
        match = Bundle(
            "match", Item.web_uri, Item.object_key, Item.content_type
        )
        matches = {
            row.match["web_uri"]: {
                "object_key": row.match["object_key"],
                "content_type": row.match["content_type"],
            }
            for row in db.query(match).filter(Item.web_uri.in_(ln_item_paths))
        }

        for ln_item in ln_items:
            match = matches.get(ln_item.link_to)

            if (
                not match
                or not match.get("object_key")
                or not match.get("content_type")
            ):
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "Unable to resolve item object_key:"
                        "\n\tURI: '%s'\n\tLink: '%s'"
                    )
                    % (ln_item.web_uri, ln_item.link_to),
                )

            ln_item.object_key = match.get("object_key")
            ln_item.content_type = match.get("content_type")


@event.listens_for(Publish, "before_update")
def publish_before_update(_mapper, _connection, publish):
    publish.updated = datetime.utcnow()


class Item(Base):
    __tablename__ = "items"

    id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    web_uri = Column(String, nullable=False)
    object_key = Column(String, nullable=True)
    content_type = Column(String, nullable=True)
    link_to = Column(String, nullable=True)
    publish_id = Column(
        UUID(as_uuid=True), ForeignKey("publishes.id"), nullable=False
    )

    publish = relationship("Publish", back_populates="items")
