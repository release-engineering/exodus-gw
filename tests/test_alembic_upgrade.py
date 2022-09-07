import mock
import pytest
from sqlalchemy.exc import OperationalError

from exodus_gw.alembic_upgrade import entry_point
from exodus_gw.models import Publish


@mock.patch("exodus_gw.database.db_engine")
def test_entry_point_alembic_upgrade(mock_db_engine, unmigrated_db):
    # Sanity check: at first there's no tables
    with pytest.raises(OperationalError):
        unmigrated_db.query(Publish).count()

    mock_db_engine.return_value = unmigrated_db.get_bind()

    # Migrate should succeed
    entry_point()

    # Now we can query stuff
    assert unmigrated_db.query(Publish).count() == 0
