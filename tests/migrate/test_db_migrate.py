"""Testing various modes of db_migrate during app startup."""
import pytest
from sqlalchemy.exc import OperationalError

from exodus_gw.migrate import db_migrate
from exodus_gw.models import Publish
from exodus_gw.settings import MigrationMode, Settings


def test_db_migrate_typical(db):
    # Sanity check: at first there's no tables
    with pytest.raises(OperationalError):
        db.query(Publish).count()

    # Migrate should succeed
    db_migrate(db.get_bind(), Settings())

    # Now we can query stuff
    assert db.query(Publish).count() == 0


def test_db_migrate_reset(db):
    # First ensure there's some tables
    db_migrate(db.get_bind(), Settings())
    assert db.query(Publish).count() == 0

    # Now if we request a reset with no migration afterward...
    db_migrate(
        db.get_bind(),
        Settings(db_reset=True, db_migration_mode=MigrationMode.none),
    )

    # ... then there should be nothing
    with pytest.raises(OperationalError):
        db.query(Publish).count()


def test_db_migrate_model(db):
    # Sanity check: at first there's no tables
    with pytest.raises(OperationalError):
        db.query(Publish).count()

    # Migrate using model should succeed
    db_migrate(db.get_bind(), Settings(db_migration_mode=MigrationMode.model))

    # Now we can query stuff
    assert db.query(Publish).count() == 0
