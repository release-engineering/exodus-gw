import pytest
from alembic.command import check, downgrade, upgrade
from alembic.config import Config
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session


def test_migrations_up_down(tmpdir, monkeypatch: pytest.MonkeyPatch):
    """Verify that "upgrade" and "downgrade" across all migrations can succeed.

    This doesn't verify functional correctness of migrations - only that the
    upgrade/downgrade functions can complete without crashing.
    """

    # Setting this env var allows migrations to insert test data to the
    # DB before/after upgrades/downgrades.
    monkeypatch.setenv("EXODUS_GW_TESTING_MIGRATIONS", "1")

    db_file = str(tmpdir.join("migration-test.db"))
    db_url = "sqlite:///%s" % db_file

    engine = create_engine(db_url)
    session = Session(bind=engine)

    config = Config()
    config.set_main_option("script_location", "exodus_gw:migrations")
    config.set_main_option("sqlalchemy.url", db_url)

    # Sanity check: we shouldn't have any tables yet
    with pytest.raises(OperationalError):
        session.execute(text("SELECT 1 FROM publishes"))

    # Upgrade to latest works
    upgrade(config, "head")

    # Sanity check: now we should have some tables
    session.execute(text("SELECT 1 FROM publishes"))

    # Downgrade to initial state works
    downgrade(config, "base")

    # Sanity check: once again no tables
    with pytest.raises(OperationalError):
        session.execute(text("SELECT 1 FROM publishes"))


def test_migration_completeness(tmpdir):
    """Verify that running all migrations results in a schema which matches
    the current sqlalchemy model.
    """

    db_file = str(tmpdir.join("migration-test.db"))
    db_url = "sqlite:///%s" % db_file

    engine = create_engine(db_url)
    session = Session(bind=engine)

    config = Config()
    config.set_main_option("script_location", "exodus_gw:migrations")
    config.set_main_option("sqlalchemy.url", db_url)

    # Sanity check: we shouldn't have any tables yet
    with pytest.raises(OperationalError):
        session.execute(text("SELECT 1 FROM publishes"))

    # Upgrade to latest works
    upgrade(config, "head")

    # Sanity check: now we should have some tables
    session.execute(text("SELECT 1 FROM publishes"))

    # If the migrations are complete, then 'check' here should succeed
    # and do nothing.
    #
    # If the migrations are missing something from the model, this should
    # detect it and raise an exception.
    check(config)
