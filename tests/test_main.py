from exodus_gw import main


def test_healthcheck():
    assert main.healthcheck() == {"detail": "exodus-gw is running"}
