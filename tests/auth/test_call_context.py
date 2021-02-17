import base64
import json

import mock
import pytest
from fastapi import HTTPException

from exodus_gw.auth import call_context
from exodus_gw.settings import Settings


@pytest.mark.asyncio
async def test_no_context():
    """Unauthenticated requests return a default context."""

    request = mock.Mock(headers={})
    request.app.state.settings = Settings()
    ctx = await call_context(request)

    # It should return a truthy object.
    assert ctx

    # It should have no roles.
    assert not ctx.client.roles
    assert not ctx.user.roles

    # It should not be authenticated.
    assert not ctx.client.authenticated
    assert not ctx.user.authenticated


@pytest.mark.asyncio
async def test_decode_context():
    """A context can be decoded from a valid header."""

    raw_context = {
        "client": {
            "roles": ["someRole", "anotherRole"],
            "authenticated": True,
            "serviceAccountId": "clientappname",
        },
        "user": {
            "roles": ["viewer"],
            "authenticated": True,
            "internalUsername": "greatUser",
        },
    }
    b64 = base64.b64encode(json.dumps(raw_context).encode("utf-8"))

    settings = Settings(call_context_header="my-auth-header")
    request = mock.Mock(headers={"my-auth-header": b64})
    request.app.state.settings = settings

    ctx = await call_context(request=request)

    # The details should match exactly the encoded data from the header.
    assert ctx.client.roles == ["someRole", "anotherRole"]
    assert ctx.client.authenticated
    assert ctx.client.serviceAccountId == "clientappname"

    assert ctx.user.roles == ["viewer"]
    assert ctx.user.authenticated
    assert ctx.user.internalUsername == "greatUser"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "header_value",
    [
        # not valid base64
        "oops not valid",
        # valid base64, but not valid JSON
        base64.b64encode(b"oops not JSON"),
        # valid base64, valid JSON, but wrong structure
        base64.b64encode(b'["oops schema mismatch]'),
    ],
)
async def test_bad_header(header_value):
    """If header does not contain valid content, a meaningful error is raised."""

    settings = Settings(call_context_header="my-auth-header")
    request = mock.Mock(headers={"my-auth-header": header_value})
    request.app.state.settings = settings

    with pytest.raises(HTTPException) as exc_info:
        await call_context(request=request)

    # It should give a 400 error (client error)
    assert exc_info.value.status_code == 400

    # It should give some hint as to what the problem is
    assert exc_info.value.detail == "Invalid my-auth-header header in request"
