"""Functions intended for use with fastapi.Depends."""

from fastapi import Depends, Request

from .auth import call_context


def get_db(request: Request):
    """DB session accessor for use with FastAPI's dependency injection system."""
    return request.state.db


# These are the preferred objects for use in endpoints,
# e.g.
#
#   db: Session = deps.db
#
db = Depends(get_db)
call_context = Depends(call_context)
