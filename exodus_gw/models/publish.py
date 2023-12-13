import uuid
from collections.abc import Sequence
from datetime import datetime
from typing import Any, Optional, Union

from fastapi import HTTPException
from sqlalchemy import (
    Boolean,
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

from exodus_gw.schemas import ItemBase

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

    def resolve_links(
        self, ln_items: Optional[Sequence[Union[ItemBase, "Item"]]] = None
    ):
        """Resolve links on publish items.

        If `ln_items` is provided, links will be resolved among these specific
        items only. Note that these can be schemas.ItemBase to resolve links
        on items on incoming requests, rather than existing items in the DB.
        Additionally, it is not a fatal error if some links in this list
        can't be resolved.

        If `ln_items` is not provided, links will be resolved for all items
        belonging to this publish object, and it's a fatal error if any
        can't be resolved.
        """

        db = inspect(self).session
        assert db

        # Whether we're doing a partial resolution, meaning it's OK for some
        # links to remain unresolved afterward.
        partial = True

        # Additional items to be used as potential link targets, on top of
        # whatever's currently in the DB.
        extra_items = []

        if ln_items is None:
            partial = False
            ln_items = (
                db.query(Item)
                .with_for_update()
                .filter(Item.publish_id == self.id)
                .filter(
                    func.coalesce(Item.link_to, "")  # pylint: disable=E1102
                    != ""
                )
                .all()
            )
        else:
            # Caller has provided specific items.
            # Divide them up into those using links and those not.
            # The items NOT using links are held onto, because those
            # are also potential candidates for link *targets*.
            extra_items = [i for i in ln_items if not i.link_to]
            ln_items = [i for i in ln_items if i.link_to]

        # Collect link targets of linked items for finding matches.
        ln_item_paths = [item.link_to for item in ln_items]

        # Store only necessary fields from matching items to conserve memory.
        match_bundle: Bundle[Any] = Bundle(
            "match", Item.web_uri, Item.object_key, Item.content_type
        )
        query = db.query(match_bundle).filter(
            Item.publish_id == self.id, Item.web_uri.in_(ln_item_paths)
        )

        matches: dict[str, dict[str, Optional[str]]] = {
            row.match.web_uri: {
                "object_key": row.match.object_key,
                "content_type": row.match.content_type,
            }
            for row in query
        }

        # If there are any extra items, they're used on top of whatever's
        # returned from the DB. This allows links to be resolved purely
        # between items provided in 'ln_items' even if the link target was
        # not saved to the DB yet.
        for ln_target in extra_items:
            if ln_target.web_uri in ln_item_paths:
                matches[ln_target.web_uri] = {
                    "object_key": ln_target.object_key,
                    "content_type": ln_target.content_type,
                }

        for ln_item in ln_items:
            assert ln_item.link_to
            match = matches.get(ln_item.link_to)

            if (
                not match
                or not match.get("object_key")
                or not match.get("content_type")
            ):
                if partial:
                    # Unresolvable links are permitted currently.
                    continue

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

            # The link has been resolved. Wipe it out so it's not resolved again.
            ln_item.link_to = None


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

    dirty: Mapped[bool] = mapped_column(Boolean, default=True)
    """True if item still needs to be written to DynamoDB."""

    updated: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow
    )
    """Last modification/creation time of the item.

    This will be eventually persisted as `from_date` on the corresponding
    DynamoDB item.
    """

    publish_id: Mapped[str] = mapped_column(
        Uuid(as_uuid=False), ForeignKey("publishes.id")
    )

    publish = relationship("Publish", back_populates="items")


@event.listens_for(Publish, "before_update")
@event.listens_for(Item, "before_update")
def set_updated(_mapper, _connection, entity: Union[Publish, Item]):
    entity.updated = datetime.utcnow()
