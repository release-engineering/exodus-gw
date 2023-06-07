import base64
import logging
from typing import List, Optional, Set

from fastapi import Depends, HTTPException, Request
from pydantic import BaseModel

LOG = logging.getLogger("exodus-gw")

# To play nice with module-local dependency injection...
# pylint: disable=redefined-outer-name


class ClientContext(BaseModel):
    """Call context data relating to service accounts / machine users."""

    roles: List[str] = []
    authenticated: bool = False
    serviceAccountId: Optional[str] = None


class UserContext(BaseModel):
    """Call context data relating to human users."""

    roles: List[str] = []
    authenticated: bool = False
    internalUsername: Optional[str] = None


class CallContext(BaseModel):
    """Represents an authenticated (or not) context for an incoming request.

    Use the fields on this model to decide whether the current request belongs
    to an authenticated user, and if so, to determine which role(s) are held
    by the user.
    """

    client: ClientContext = ClientContext()
    user: UserContext = UserContext()


async def call_context(request: Request) -> CallContext:
    """Returns the CallContext for the current request."""

    settings = request.app.state.settings
    header = settings.call_context_header
    header_value = request.headers.get(header)
    if not header_value:
        return CallContext()

    try:
        decoded = base64.b64decode(header_value, validate=True)
        return CallContext.parse_raw(decoded)
    except Exception:
        summary = "Invalid %s header in request" % header
        LOG.exception(summary, extra={"event": "auth", "success": False})
        raise HTTPException(400, detail=summary) from None


async def caller_name(context: CallContext = Depends(call_context)) -> str:
    """Returns the name(s) of the calling user and/or serviceaccount.

    The returned value is appropriate only for logging.
    """

    # No idea whether it's actually possible for a request to be authenticated
    # as both a user and a serviceaccount, but the design of the call context
    # structure allows for it, so this code will tolerate it also.
    users = []
    if context.user.internalUsername:
        users.append(f"user {context.user.internalUsername}")
    if context.client.serviceAccountId:
        users.append(f"serviceaccount {context.client.serviceAccountId}")
    if not users:
        users.append("<anonymous user>")

    return " AND ".join(users)


async def caller_roles(
    context: CallContext = Depends(call_context),
) -> Set[str]:
    """Returns all roles held by the caller of the current request.

    This will be an empty set for unauthenticated requests.
    """
    return set(context.user.roles + context.client.roles)


def needs_role(rolename):
    """Returns a dependency on a specific named role.

    This function is intended to be used with "dependencies" on endpoints in
    order to associate them with specific roles. Requests to that endpoint will
    fail unless the caller is authenticated as a user having that role.

    For example:

    > @app.post('/my-great-api/frobnitz', dependencies=[needs_role("xyz")])
    > def do_frobnitz():
    >    "If caller does not have role xyz, they will never get here."
    """

    async def check_roles(
        request: Request,
        env: Optional[str] = None,
        roles: Set[str] = Depends(caller_roles),
        caller_name: str = Depends(caller_name),
    ):
        role = env + "-" + rolename if env else rolename

        if role not in roles:
            LOG.warning(
                "Access denied; path=%s, user=%s, role=%s",
                request.base_url.path,
                caller_name,
                role,
                extra={"event": "auth", "success": False},
            )
            raise HTTPException(
                403, "this operation requires role '%s'" % role
            )

        LOG.info(
            "Access permitted; path=%s, user=%s, role=%s",
            request.base_url.path,
            caller_name,
            role,
            extra={"event": "auth", "success": True},
        )

    return Depends(check_roles)
