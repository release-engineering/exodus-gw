import logging

from alembic.command import upgrade
from alembic.config import Config
from sqlalchemy import MetaData
from sqlalchemy.engine import Engine

from exodus_gw import models
from exodus_gw.database import db_url
from exodus_gw.settings import MigrationMode, Settings

LOG = logging.getLogger("exodus-gw")


def db_reset(db: Engine):
    LOG.warning("Resetting database!")

    meta = MetaData()

    # This finds all existing tables (in public schema; does not include dramatiq queue)
    meta.reflect(bind=db)

    LOG.warning("Dropping table(s): [%s]", ", ".join(meta.tables.keys()))

    # This drops them all
    meta.drop_all(bind=db)

    LOG.warning("Database emptied!")


def db_migrate(db: Engine, settings: Settings):
    if settings.db_reset:
        db_reset(db)

    revision = settings.db_migration_revision
    config = Config()
    config.set_main_option("script_location", "exodus_gw:migrations")
    config.set_main_option("sqlalchemy.url", db_url(settings))

    if settings.db_migration_mode == MigrationMode.none:
        LOG.warning("DB initialization skipped")
    elif settings.db_migration_mode == MigrationMode.upgrade:
        LOG.info("Upgrading DB to %s", revision)
        upgrade(config, revision)
    elif settings.db_migration_mode == MigrationMode.model:
        LOG.warning(
            "Using DB model instead of migrations. Do not use in production!"
        )
        models.Base.metadata.create_all(bind=db)
