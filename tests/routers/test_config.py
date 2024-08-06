import pytest
from fastapi.testclient import TestClient

from exodus_gw.main import app


def test_config_get(auth_header, fake_config, mock_boto3_client):
    with TestClient(app) as client:
        r = client.get(
            "/test/config",
            headers=auth_header(roles=["test-config-consumer"]),
        )

    # It should have succeeded and returned stored config
    assert r.status_code == 200
    assert r.json() == fake_config
