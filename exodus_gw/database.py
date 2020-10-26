from sqlalchemy import create_engine

from .settings import get_settings


def get_db():
    """Get db engine
    The database name is identical to the user name
    See additional info:
    https://devopstuto-docker.readthedocs.io/en/latest/docker_library/postgresql/postgresql.html#postgres-user # noqa
    """
    SQLALCHEMY_DATABASE_URL = (
        "postgresql://{s.db_service_user}:{s.db_service_pass}@"
        "{s.db_service_host}:{s.db_service_port}/{s.db_service_user}"
    ).format(s=get_settings())
    return create_engine(SQLALCHEMY_DATABASE_URL)
