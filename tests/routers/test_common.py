"""Tests some invariants which are common to most/all endpoints."""

import json

import pytest
from fastapi.routing import APIRoute
from fastapi.testclient import TestClient
from freezegun import freeze_time

from exodus_gw.main import app


def all_api_routes():
    return [r for r in app.routes if isinstance(r, APIRoute)]


@pytest.fixture(params=all_api_routes(), ids=lambda route: route.path)
def api_route(request):
    yield request.param


def test_has_tags(api_route):
    """Every route declares some tag."""
    assert api_route.tags


def test_has_response_model(api_route):
    """Every route declares a response model.

    This is required to ensure that all responses are serialized according
    to a predefined schema.
    """

    # Upload APIs are exempt from this because they must implement S3 compatibility
    # which is not achieved via any pydantic response model
    if api_route.path.startswith("/upload/"):
        pytest.skip("Not applicable for upload APIs")

    assert api_route.response_model


def test_requires_auth(api_route):
    """Most routes require auth."""

    # Only a handful of allowlisted paths can be used without authentication:
    if api_route.path in [
        # healthchecks should be usable without auth so anyone can verify the
        # system is up
        "/healthcheck",
        "/healthcheck-worker",
        # this should not need auth as the endpoint is designed to tell you
        # whether or not you're authorized
        "/whoami",
        # FIXME? We do not require auth for this, but maybe we should.
        # Unauthenticated users have no way to find these tasks or to know what
        # they represent. It'd be safest to lock this down too.
        "/task/{task_id}",
        # authorization for the CDN is handled elsewhere, by other means, we
        # don't want to restrict it in Exodus gateway
        "/{env}/cdn/{url:path}",
    ]:
        pytest.skip("auth not required")

    # In any other case, the endpoint must declare some dependency on the
    # role checker.
    assert "Depends(check_roles)" in repr(api_route.dependencies)


@freeze_time("2023-07-28 13:24:03.597+00:00")
@pytest.mark.parametrize(
    "endpoint,user,roles",
    [
        ("/healthcheck", "<anonymous user>", set()),
        ("/healthcheck", "user fake-user", "{'test-publisher'}"),
        ("/foo/publish", "user fake-user", "{'test-publisher'}"),
    ],
    ids=[
        "anon-auth-not-required",
        "authenticated-auth-not-required",
        "authenticated-auth-required",
    ],
)
def test_login_log(endpoint, user, roles, caplog, auth_header):
    """Every route produces a log describing a login event."""
    with TestClient(app) as client:
        if roles:
            if endpoint == "/foo/publish":
                client.post(
                    endpoint, headers=auth_header(roles=["test-publisher"])
                )
            else:
                client.get(
                    endpoint, headers=auth_header(roles=["test-publisher"])
                )
        else:
            client.get(endpoint)
        expected_log = {
            "level": "INFO",
            "logger": "exodus-gw",
            "time": "2023-07-28 13:24:03.596",
            "message": f"Login: path={endpoint}, user={user}, roles={roles}",
            "event": "login",
            "success": True,
        }
        if user == "<anonymous user>":
            assert expected_log not in [
                json.loads(line) for line in caplog.text.splitlines()
            ]
        else:
            assert expected_log in [
                json.loads(line) for line in caplog.text.splitlines()
            ]
