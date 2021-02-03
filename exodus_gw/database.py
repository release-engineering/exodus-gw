from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base

from .settings import Settings

Base = declarative_base()


def db_url(settings: Settings):
    if settings.db_url:
        return settings.db_url

    return (
        "postgresql://{s.db_service_user}:{s.db_service_pass}@"
        "{s.db_service_host}:{s.db_service_port}/{s.db_service_user}"
    ).format(s=settings)


def db_engine(settings: Settings):
    return create_engine(db_url(settings))
