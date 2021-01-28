"""APIs for inspecting the state of the exodus-gw service."""

import logging

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from .. import worker
from ..auth import CallContext, call_context
from ..database import get_db

LOG = logging.getLogger("exodus-gw")

openapi_tag = {"name": "service", "description": __doc__}

router = APIRouter(tags=[openapi_tag["name"]])


@router.get("/healthcheck")
def healthcheck():
    """Returns a successful response if the service is running."""
    return {"detail": "exodus-gw is running"}


@router.get("/healthcheck-worker")
def healthcheck_worker(db: Session = Depends(get_db)):
    """Returns a successful response if background workers are running."""

    msg = worker.ping.send()

    # Message would not normally be sent until commit after the request succeeds.
    # Since we want to get the result, we'll commit early.
    db.commit()

    # If we don't get a response in time, this will raise an exception and we'll
    # respond with a 500 error, which seems reasonable.
    result = msg.get_result(block=True, timeout=5000)

    return {"detail": "background worker is running: ping => %s" % result}


@router.get(
    "/whoami",
    response_model=CallContext,
    responses={
        200: {
            "description": "returns caller's auth context",
            "content": {
                "application/json": {
                    "example": {
                        "client": {
                            "roles": ["someRole", "anotherRole"],
                            "authenticated": True,
                            "serviceAccountId": "clientappname",
                        },
                        "user": {
                            "roles": ["viewer"],
                            "authenticated": True,
                            "internalUsername": "someuser",
                        },
                    }
                }
            },
        }
    },
)
def whoami(context: CallContext = Depends(call_context)):
    """Return basic information on the caller's authentication & authorization context.

    This endpoint may be used to determine whether the caller is authenticated to
    the exodus-gw service, and if so, which set of role(s) are held by the caller.

    It is a read-only endpoint intended for diagnosing authentication issues.
    """
    return context
