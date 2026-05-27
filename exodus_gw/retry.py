import logging

import backoff
import dramatiq
from fastapi import Request
from fastapi.routing import APIRoute
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import Session
from starlette.concurrency import run_in_threadpool

LOG = logging.getLogger("exodus-gw")


def new_db_session(engine):
    # Make a new DB session for use in the current request.
    #
    # This is in its own function so that it can be wrapped by tests.
    return Session(bind=engine, autoflush=False, autocommit=False)


async def db_session(request: Request, call_next):
    """Maintain a DB session around each request, which is also shared
    with the dramatiq broker.

    An implicit commit occurs if and only if the request succeeds.
    """

    request.state.db = new_db_session(request.app.state.db_engine)

    # Any dramatiq operations should also make use of this session.
    broker = dramatiq.get_broker()
    broker.set_session(request.state.db)  # type: ignore
    try:
        response = await call_next(request)
        if response.status_code >= 200 and response.status_code < 300:
            await run_in_threadpool(request.state.db.commit)
    finally:
        # Check if RetryRoute has already cleaned up the session
        if request.state.db is not None:
            # If not, the session should be cleaned up.
            broker.set_session(None)  # type: ignore
            await run_in_threadpool(request.state.db.close)
            request.state.db = None
    return response


class RetryRoute(APIRoute):
    def get_route_handler(self):
        original_route_handler = super().get_route_handler()

        async def retry_route_handler(request: Request):
            max_tries = request.app.state.settings.db_session_max_tries

            @backoff.on_exception(
                backoff.expo, DBAPIError, max_tries=max_tries
            )
            async def retry_wrapper():
                broker = dramatiq.get_broker()
                if request.state.db is None:
                    # Create new DB session if last one had an error
                    request.state.db = new_db_session(
                        request.app.state.db_engine
                    )
                    broker.set_session(request.state.db)

                try:
                    return await original_route_handler(request)

                except DBAPIError:
                    # Rollback and clear DB session
                    await run_in_threadpool(request.state.db.rollback)
                    broker.set_session(None)
                    await run_in_threadpool(request.state.db.close)
                    request.state.db = None
                    raise

            return await retry_wrapper()

        return retry_route_handler
