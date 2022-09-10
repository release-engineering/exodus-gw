#
# A wrapper for "alembic upgrade head" which will run upgrade operations to
# the latest revision.
#

from .database import db_engine
from .migrate import db_migrate
from .settings import MigrationMode, load_settings


def entry_point():
    settings = load_settings()

    settings.db_migration_mode = MigrationMode.upgrade
    settings.db_migration_revision = "head"

    engine = db_engine(settings)
    db_migrate(engine, settings)

    print("DB migration finished.")
