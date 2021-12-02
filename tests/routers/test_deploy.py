import pytest
from fastapi.testclient import TestClient

from exodus_gw.main import app


def test_deploy_config_typical(fake_config, auth_header):
    with TestClient(app) as client:
        r = client.post(
            "/test/deploy-config",
            json=fake_config,
            headers=auth_header(roles=["test-config-deployer"]),
        )

    # It should have succeeded and returned a task object
    assert r.ok

    task_id = r.json()["id"]
    assert r.json()["links"] == {"self": "/task/%s" % task_id}


@pytest.mark.parametrize(
    "data",
    [
        {
            "listing": {
                "/origin/../rhel/server": {
                    "values": ["8"],
                    "var": "releasever",
                }
            }
        },
        {"listing": {"/origin/rhel/server": {"values": ["8"], "var": "nope"}}},
        {
            "rhui_alias": [
                {"dest": "/../rhel/rhui/server", "src": "/../rhel/rhui/server"}
            ]
        },
        {"no_dont": 123},
    ],
    ids=[
        "listing_path",
        "listing_var",
        "alis_path",
        "additional_property",
    ],
)
def test_deploy_config_bad_config(data, fake_config, auth_header):
    # Add bad config data.
    fake_config.update(data)

    with TestClient(app) as client:
        r = client.post(
            "/test/deploy-config",
            json=fake_config,
            headers=auth_header(roles=["test-config-deployer"]),
        )

    # It should have failed
    assert r.status_code == 400
    assert r.json() == {"detail": "Invalid configuration structure"}
