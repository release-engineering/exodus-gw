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


def test_config_get_empty_config(auth_header, mock_boto3_client_empty_config):
    with TestClient(app) as client:
        r = client.get(
            "/test/config",
            headers=auth_header(roles=["test-config-consumer"]),
        )

    # It should have succeeded and returned the default empty config
    assert r.status_code == 200
    assert r.json() == {
        "listing": {},
        "origin_alias": [],
        "releasever_alias": [],
        "rhui_alias": [],
    }


@pytest.mark.parametrize("endpoint", ["config", "deploy-config"])
def test_deploy_config_typical(fake_config, auth_header, endpoint):
    with TestClient(app) as client:
        r = client.post(
            f"/test/{endpoint}",
            json=fake_config,
            headers=auth_header(roles=["test-config-deployer"]),
        )

    # It should have succeeded and returned a task object
    assert r.status_code == 200

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
                {
                    "dest": "/../rhel/rhui/server",
                    "src": "/../rhel/rhui/server",
                    "exclude_paths": ["/this/", "/is/", "/fine/"],
                }
            ]
        },
        {"no_dont": 123},
    ],
    ids=[
        "listing_path",
        "listing_var",
        "alias_path",
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
