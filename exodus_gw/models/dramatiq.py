import datetime
from typing import Any

from sqlalchemy import DateTime, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.types import Uuid

from .base import Base

# Avoid pylint complaining about:
# E1136: Value 'Mapped' is unsubscriptable
# These errors started to appear around 2024-08, and they're wrong.
# pylint: disable=unsubscriptable-object


class DramatiqMessage(Base):
    # This table holds queued messages.
    # Messages are deleted from the queue once successfully processed.

    __tablename__ = "dramatiq_messages"

    # ID of message
    id: Mapped[str] = mapped_column(
        Uuid(as_uuid=False),
        primary_key=True,
    )

    # ID of consumer who has assigned this message to itself.
    # Null means message is not yet assigned.
    # Not a foreign key since consumers can disappear while leaving
    # their messages behind.
    consumer_id: Mapped[str | None] = mapped_column(String)

    consumer = relationship(
        "DramatiqConsumer",
        foreign_keys=[consumer_id],
        primaryjoin="DramatiqMessage.consumer_id == DramatiqConsumer.id",
    )

    # Name of queue (e.g. "default" for most messages)
    queue: Mapped[str] = mapped_column(String)

    # Name of actor
    actor: Mapped[str] = mapped_column(String)

    # Full message body.
    body: Mapped[dict[str, Any]] = mapped_column(JSONB)


class DramatiqConsumer(Base):
    # This table holds one record for each live consumer.
    # It's mainly used for the system to detect and recover from
    # dead consumers.

    __tablename__ = "dramatiq_consumers"

    # Unique ID of consumer. Consumers set a new unique ID every
    # time they start up.
    id: Mapped[str] = mapped_column(
        String,
        primary_key=True,
    )

    # Last time this consumer reported itself to be alive.
    last_alive: Mapped[datetime.datetime] = mapped_column(DateTime)
