from sqlalchemy.engine import Engine

from exodus_gw import database


def test_get_db():
    assert isinstance(database.get_db(), Engine)
