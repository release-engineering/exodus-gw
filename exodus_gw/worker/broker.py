import logging
import os
from contextvars import ContextVar

import dramatiq_pg
from dramatiq.brokers.stub import StubBroker
from dramatiq.results import Results
from dramatiq.results.backends import StubBackend
from dramatiq_pg.utils import transaction
from psycopg2.errors import DuplicateSchema  # pylint: disable=E0611

from exodus_gw.database import SQLALCHEMY_DATABASE_URL

LOG = logging.getLogger("exodus-gw")

SCHEMA_PATH = os.path.join(os.path.dirname(__file__), "schema.sql")


class SessionPoolAdapter:
    """Adapts an sqlalchemy session into a connection pool compatible
    with dramatiq-pg.
    """

    def __init__(self, session):
        self._session = session

    def execute(self, statement, parameters=None):
        # When requested to execute a statement, we go through sqlalchemy
        # so that we're sharing the same connection & transaction as the ORM.
        #
        # Note: change this to Connection.exec_driver_sql()
        # once we are on sqlalchemy >= 1.4
        return self._session.connection().execute(statement, parameters)

    # The empty implementations of below methods effectively cause us to ignore
    # requests to begin or end any transactions - we want to always leave that
    # up to the sqlalchemy session.
    def __enter__(self):
        return self

    def __exit__(self, *_):
        pass

    def cursor(self):
        return self


class NoSessionAvailable:
    """Helper to raise a meaningful error if broker is unexpectedly used outside
    the context of an sqlalchemy session.
    """

    def __enter__(self):
        raise RuntimeError(
            "BUG: attempted to use session-aware broker while no session is active!"
        )

    def __exit__(self, *_):
        # If __exit__ is not implemented, we get a crash during tests, yet pytest-cov
        # believes that this is not covered - not sure what's the deal here.
        pass  # pragma: no cover


class ExodusGwBroker(
    dramatiq_pg.PostgresBroker
):  # pylint: disable=abstract-method
    """Dramatiq broker with customizations for exodus-gw:

    - uses DB from settings by default, same as rest of exodus-gw
    - initializes required tables on first use
    - can join the broker to an existing sqlalchemy session
    """

    def __init__(self, url=SQLALCHEMY_DATABASE_URL, pool=None):
        # Since we only have one globally shared broker, but
        # we want to connect with an sqlalchemy session which is
        # specific to one coro, we'll make a context-aware wrapper
        # for pool.
        self.__pool = ContextVar("pool")

        # Uses the same DB as used for sqlalchemy by default.
        super().__init__(url=url, pool=pool)

    @property
    def pool(self):
        return self.__pool.get()

    @pool.setter
    def pool(self, value):
        self.__pool.set(value)

    def set_session(self, session):
        """Set an sqlalchemy session for use with the broker.

        - When first constructed, the broker manages its own connection and transactions
          with postgres.

        - If set_session is called with a session, the broker will execute all subsequent
          SQL statements via that session, and will no longer perform any management of
          transactions (any COMMIT or ROLLBACK must occur externally from the broker).

        - If set_session is called with None, the broker becomes unusable until another
          session is set, i.e. if you use the broker with a session once, you must continue
          to use it that way permanently. This is enforced to avoid mixing session-aware
          and non-session-aware usage of the broker which would be a source of bugs.
        """
        if session:
            self.pool = SessionPoolAdapter(session)
        else:
            self.pool = NoSessionAvailable()

    def __ensure_init(self):
        with open(SCHEMA_PATH, "rt") as f:
            schema = f.read()

        LOG.info("Ensuring dramatiq schema at %s is applied", SCHEMA_PATH)

        try:
            with transaction(self.pool) as cursor:
                cursor.execute(schema)
            LOG.info("dramatiq schema applied")
        except DuplicateSchema:
            LOG.info("dramatiq schema was already in place")

    def consume(self, *args, **kwargs):  # pylint: disable=signature-differs
        # compared to the default broker, we'll automatically
        # initialize our schema on worker startup rather than
        # requiring a separate command for this.
        self.__ensure_init()

        return super().consume(*args, **kwargs)


class ExodusGwStubBroker(StubBroker):
    def __init__(self):
        super().__init__()
        # Default stub broker doesn't enable any results backend by default,
        # while we need one
        self.add_middleware(Results(backend=StubBackend()))

    def set_session(self, _session):
        # Implement a no-op for this method just to be API compatible
        # with the real broker.
        pass


def new_broker():
    # As recommended in dramatiq docs, returns a StubBroker if we are
    # currently running tests. This env var is set in conftest.py prior
    # to import.
    if os.getenv("EXODUS_GW_STUB_BROKER") == "1":
        return ExodusGwStubBroker()

    return ExodusGwBroker()
