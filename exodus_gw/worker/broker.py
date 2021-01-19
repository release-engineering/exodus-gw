import logging
import os

import dramatiq_pg
from dramatiq.brokers.stub import StubBroker
from dramatiq.results import Results
from dramatiq.results.backends import StubBackend
from dramatiq_pg.utils import transaction
from psycopg2.errors import DuplicateSchema  # pylint: disable=E0611

from exodus_gw.database import SQLALCHEMY_DATABASE_URL

LOG = logging.getLogger("exodus-gw")

SCHEMA_PATH = os.path.join(os.path.dirname(__file__), "schema.sql")


class ExodusGwBroker(
    dramatiq_pg.PostgresBroker
):  # pylint: disable=abstract-method
    """Dramatiq broker with customizations for exodus-gw:

    - uses DB from settings by default, same as rest of exodus-gw
    - initializes required tables on first use
    """

    def __init__(self, url=SQLALCHEMY_DATABASE_URL, pool=None):
        # Uses the same DB as used for sqlalchemy by default.
        super().__init__(url=url, pool=pool)

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


def new_broker():
    # As recommended in dramatiq docs, returns a StubBroker if we are
    # currently running tests. This env var is set in conftest.py prior
    # to import.
    if os.getenv("EXODUS_GW_STUB_BROKER") == "1":
        out = StubBroker()
        out.add_middleware(Results(backend=StubBackend()))
        return out

    return ExodusGwBroker()
