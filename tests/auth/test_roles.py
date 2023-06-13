import logging
from dataclasses import dataclass

import pytest
from fastapi import HTTPException
from starlette.datastructures import URL

from exodus_gw.auth import (
    CallContext,
    ClientContext,
    UserContext,
    caller_name,
    caller_roles,
    needs_role,
)


@dataclass
class FakeRequest:
    url: URL


async def test_caller_roles_empty():
    """caller_roles returns an empty set for a default (empty) context."""

    assert (await caller_roles(CallContext())) == set()


async def test_caller_roles_nonempty():
    """caller_roles returns all roles from the context when present."""

    ctx = CallContext(
        user=UserContext(roles=["role1", "role2"]),
        client=ClientContext(roles=["role2", "role3"]),
    )
    assert (await caller_roles(ctx)) == set(["role1", "role2", "role3"])


async def test_caller_name_empty():
    """caller_name returns a reasonable value for an unauthed context."""

    assert (await caller_name(CallContext())) == "<anonymous user>"


async def test_caller_name_simple():
    """caller_name returns a reasonable value for a typical authed context."""

    assert (
        await caller_name(
            CallContext(user=UserContext(internalUsername="shazza"))
        )
    ) == "user shazza"


async def test_caller_name_multi():
    """caller_name returns a reasonable value for a context having both user
    and serviceaccount authentication info.

    (Unclear if this can really happen.)
    """

    assert (
        await caller_name(
            CallContext(
                user=UserContext(internalUsername="shazza"),
                client=ClientContext(serviceAccountId="bottle-o"),
            )
        )
    ) == "user shazza AND serviceaccount bottle-o"


async def test_needs_role_success(caplog: pytest.LogCaptureFixture):
    """needs_role succeeds and logs needed role is present."""

    caplog.set_level(logging.INFO)

    fn = needs_role("better-role").dependency

    # It should succeed
    await fn(
        FakeRequest(URL("/endpoint")),
        roles=set(["better-role"]),
        caller_name="bazza",
    )

    # It should log about the successful auth
    assert (
        "Access permitted; path=/endpoint, user=bazza, role=better-role"
        in caplog.text
    )


async def test_needs_role_fail(caplog: pytest.LogCaptureFixture):
    """needs_role logs and raises meaningful error when needed role is absent."""

    fn = needs_role("best-role").dependency

    # It should raise an exception.
    with pytest.raises(HTTPException) as exc_info:
        await fn(
            FakeRequest(URL("/endpoint")),
            roles=set(["abc", "xyz"]),
            caller_name="dazza",
        )

    # It should use status 403 to tell the client they are unauthorized.
    assert exc_info.value.status_code == 403

    # It should give some hint as to the needed role.
    assert exc_info.value.detail == "this operation requires role 'best-role'"

    # It should log about the authorization failure
    assert (
        "Access denied; path=/endpoint, user=dazza, role=best-role"
        in caplog.text
    )
