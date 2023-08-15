from exodus_gw.database import db_url
from exodus_gw.settings import Settings


def test_url_from_components():
    settings = Settings(
        db_service_user="user",
        db_service_pass="pass",
        db_service_host="somehost",
        db_service_port="1122",
        db_url=None,
    )
    assert db_url(settings) == "postgresql://user:pass@somehost:1122/user"


def test_url_from_url():
    settings = Settings(
        db_service_user="user",
        db_service_pass="pass",
        db_service_host="somehost",
        db_service_port="1122",
        db_url="some://great-db",
    )
    assert db_url(settings) == "some://great-db"
