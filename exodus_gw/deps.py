"""Functions intended for use with fastapi.Depends."""

import logging
import sys
from asyncio import LifoQueue

from fastapi import Depends, Path, Request

from .auth import call_context as get_call_context
from .aws.client import S3ClientWrapper
from .settings import Environment, Settings, get_environment

LOG = logging.getLogger("exodus-gw")

# Because we cannot rename arguments to silence this pylint warning
# without breaking the dependency injection system...
# pylint: disable=redefined-outer-name


async def get_db(request: Request):
    """DB session accessor for use with FastAPI's dependency injection system."""
    return request.state.db


async def get_settings(request: Request):
    return request.app.state.settings


async def get_environment_from_path(
    env: str = Path(
        ...,
        title="environment",
        description="[Environment](#section/Environments) on which to operate.",
    ),
    settings: Settings = Depends(get_settings),
):
    return get_environment(env, settings)


async def queue_for_profile(profile: str, maxsize: int):
    # Create a queue of duplicate s3 clients for the given AWS profile.
    # Client duplication is a procautionary measure against potential
    # overwriting at runtime.

    # mypy and pylint have trouble agreeing on how to handle LifoQueue type...
    queue = LifoQueue(maxsize=maxsize)  # type: ignore

    while not queue.full():
        client = await S3ClientWrapper(profile=profile).__aenter__()
        queue.put_nowait(client)

    return queue


async def get_s3_client(
    request: Request,
    env: Environment = Depends(get_environment_from_path),
    settings: Settings = Depends(get_settings),
):
    # Produce an active s3 client from the queue for the given environment.

    profile = env.aws_profile
    queue_size = settings.s3_pool_size
    queues = request.app.state.s3_queues

    if profile not in queues:
        queues[profile] = await queue_for_profile(profile, queue_size)

    queue = queues[profile]
    client = await queue.get()

    try:
        LOG.debug(
            "Request %s using S3 client %s",
            request.scope.get("path"),
            client,
            extra={"event": "deps"},
        )
        yield client
    except Exception:
        # When an exception is raised, assume the client broke.
        # Close the client and replace it before raising.
        await client.__aexit__(*sys.exc_info())
        client = await S3ClientWrapper(profile=profile).__aenter__()
        raise
    finally:
        await queue.put(client)


# These are the preferred objects for use in endpoints,
# e.g.
#
#   db: Session = deps.db
#
db = Depends(get_db)
call_context = Depends(get_call_context)
env = Depends(get_environment_from_path)
settings = Depends(get_settings)
s3_client = Depends(get_s3_client)
