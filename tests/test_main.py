import mock

from exodus_gw.database import engine
from exodus_gw.main import db_init


def test_db_init() -> None:
    with mock.patch(
        "exodus_gw.models.Base.metadata.create_all"
    ) as mock_method:
        db_init()
        mock_method.assert_called_once_with(bind=engine)
