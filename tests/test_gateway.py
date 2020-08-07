from exodus_gw import gateway


def test_healthcheck():
    assert gateway.healthcheck() == {"detail": "exodus-gw is running"}
