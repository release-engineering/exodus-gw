import pytest

from fastapi import HTTPException

from exodus_gw.auth import (
    CallContext,
    ClientContext,
    UserContext,
    caller_roles,
    needs_role,
)


def test_caller_roles_empty():
    """caller_roles returns an empty set for a default (empty) context."""

    assert caller_roles(CallContext()) == set()


def test_caller_roles_nonempty():
    """caller_roles returns all roles from the context when present."""

    ctx = CallContext(
        user=UserContext(roles=["role1", "role2"]),
        client=ClientContext(roles=["role2", "role3"]),
    )
    assert caller_roles(ctx) == set(["role1", "role2", "role3"])


def test_needs_role_success():
    """needs_role succeeds when needed role is present."""

    fn = needs_role("better-role").dependency

    # It should do nothing, successfully
    fn(roles=set(["better-role"]))


def test_needs_role_fail():
    """needs_role raises meaningful error when needed role is absent."""

    fn = needs_role("best-role").dependency

    # It should raise an exception.
    with pytest.raises(HTTPException) as exc_info:
        fn(roles=set(["abc", "xyz"]))

    # It should use status 403 to tell the client they are unauthorized.
    assert exc_info.value.status_code == 403

    # It should give some hint as to the needed role.
    assert exc_info.value.detail == "this operation requires role 'best-role'"
