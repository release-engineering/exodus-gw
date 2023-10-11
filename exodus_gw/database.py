import logging

from sqlalchemy import create_engine, event
from sqlalchemy.orm import DeclarativeBase

from .settings import Settings

LOG = logging.getLogger("exodus-gw.db")

Base = DeclarativeBase()


def db_url(settings: Settings):
    if settings.db_url:
        return settings.db_url

    return (
        "postgresql://{s.db_service_user}:{s.db_service_pass}@"
        "{s.db_service_host}:{s.db_service_port}/{s.db_service_user}"
    ).format(s=settings)


def db_engine(settings: Settings):
    engine = create_engine(db_url(settings), pool_pre_ping=True)

    # Arrange for connection logs at INFO level.
    # Note, extracting the URL from the engine rather than reusing db_url
    # is needed to ensure password is masked.
    event.listen(
        engine,
        "connect",
        lambda *_: LOG.info("Connecting to database: %s", engine.url),
    )

    return engine
