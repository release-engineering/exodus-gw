from sqlalchemy import Column, DateTime, String
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import relationship

from .base import Base


class DramatiqMessage(Base):
    # This table holds queued messages.
    # Messages are deleted from the queue once successfully processed.

    __tablename__ = "dramatiq_messages"

    # ID of message
    id = Column(
        UUID(as_uuid=True),
        primary_key=True,
    )

    # ID of consumer who has assigned this message to itself.
    # Null means message is not yet assigned.
    # Not a foreign key since consumers can disappear while leaving
    # their messages behind.
    consumer_id = Column(String, nullable=True)

    consumer = relationship(
        "DramatiqConsumer",
        foreign_keys=[consumer_id],
        primaryjoin="DramatiqMessage.consumer_id == DramatiqConsumer.id",
    )

    # Name of queue (e.g. "default" for most messages)
    queue = Column(String, nullable=False)

    # Full message body.
    body = Column(JSONB, nullable=False)


class DramatiqConsumer(Base):
    # This table holds one record for each live consumer.
    # It's mainly used for the system to detect and recover from
    # dead consumers.

    __tablename__ = "dramatiq_consumers"

    # Unique ID of consumer. Consumers set a new unique ID every
    # time they start up.
    id = Column(
        String,
        primary_key=True,
    )

    # Last time this consumer reported itself to be alive.
    last_alive = Column(DateTime, nullable=False)
