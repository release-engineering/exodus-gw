from exodus_gw import gateway


def test_healthcheck():
    assert gateway.healthcheck() == {"200": "OK"}
