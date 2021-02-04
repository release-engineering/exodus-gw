"""Functions intended for use with fastapi.Depends."""

from fastapi import Depends, Path, Request

from .auth import call_context
from .settings import Settings, get_environment


def get_db(request: Request):
    """DB session accessor for use with FastAPI's dependency injection system."""
    return request.state.db


def get_settings(request: Request):
    return request.app.state.settings


def get_environment_from_path(
    env: str = Path(
        ...,
        title="environment",
        description="[Environment](#section/Environments) on which to operate.",
    ),  # pylint: disable=redefined-outer-name
    settings: Settings = Depends(get_settings),
):
    return get_environment(env, settings)


# These are the preferred objects for use in endpoints,
# e.g.
#
#   db: Session = deps.db
#
db = Depends(get_db)
call_context = Depends(call_context)
env = Depends(get_environment_from_path)
settings = Depends(get_settings)
