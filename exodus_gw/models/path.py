from datetime import datetime

from sqlalchemy import Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from exodus_gw.types import UTCDateTime

from .base import Base


# pylint: disable=unsubscriptable-object
class PublishedPath(Base):
    """Represents a path updated on the CDN at some point.

    This table keeps a lightweight history of paths previously updated
    by exodus-gw. The history is intended to support cache flushing
    use-cases only and does not contain a full history of every updated
    path.
    """

    __tablename__ = "published_paths"
    __table_args__ = (
        UniqueConstraint(
            "env", "web_uri", name="published_paths_env_web_uri_key"
        ),
    )

    id: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=True
    )

    env: Mapped[str] = mapped_column(String)
    """Env on which this path was updated (e.g. 'pre', 'live')"""

    web_uri: Mapped[str] = mapped_column(String)
    """An updated path, e.g. /content/dist/some/repodata/repomd.xml"""

    updated: Mapped[datetime] = mapped_column(UTCDateTime)
    """Last time this path was updated."""
