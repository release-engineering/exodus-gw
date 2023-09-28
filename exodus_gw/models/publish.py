import uuid
from datetime import datetime
from typing import Optional

from fastapi import HTTPException
from sqlalchemy import (
    DateTime,
    ForeignKey,
    String,
    UniqueConstraint,
    event,
    func,
    inspect,
)
from sqlalchemy.orm import Bundle, Mapped, mapped_column, relationship
from sqlalchemy.types import Uuid

from .base import Base


class Publish(Base):
    __tablename__ = "publishes"

    id: Mapped[str] = mapped_column(
        Uuid(as_uuid=False),
        primary_key=True,
        default=lambda: str(uuid.uuid4()),
    )
    env: Mapped[str] = mapped_column(String)
    state: Mapped[str] = mapped_column(String)
    updated: Mapped[Optional[datetime]] = mapped_column(DateTime())
    items = relationship(
        "Item", back_populates="publish", cascade="all, delete-orphan"
    )

    def resolve_links(self):
        db = inspect(self).session
        # Store only publish items with link targets.
        ln_items = (
            db.query(Item)
            .filter(Item.publish_id == self.id)
            .filter(
                func.coalesce(Item.link_to, "") != ""  # pylint: disable=E1102
            )
            .all()
        )
        # Collect link targets of linked items for finding matches.
        ln_item_paths = [item.link_to for item in ln_items]

        # Store only necessary fields from matching items to conserve memory.
        match = Bundle(
            "match", Item.web_uri, Item.object_key, Item.content_type
        )
        matches = {
            row.match.web_uri: {
                "object_key": row.match.object_key,
                "content_type": row.match.content_type,
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
    __table_args__ = (
        UniqueConstraint(
            "publish_id", "web_uri", name="items_publish_id_web_uri_key"
        ),
    )

    id: Mapped[str] = mapped_column(
        Uuid(as_uuid=False),
        primary_key=True,
        default=lambda: str(uuid.uuid4()),
    )
    web_uri: Mapped[str] = mapped_column(String)
    object_key: Mapped[Optional[str]] = mapped_column(String)
    content_type: Mapped[Optional[str]] = mapped_column(String)
    link_to: Mapped[Optional[str]] = mapped_column(String)
    publish_id: Mapped[str] = mapped_column(
        Uuid(as_uuid=False), ForeignKey("publishes.id")
    )

    publish = relationship("Publish", back_populates="items")
