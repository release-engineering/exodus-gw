import logging
import os

from dramatiq import Middleware
from dramatiq_pg.utils import transaction

LOG = logging.getLogger("exodus-gw")
SCHEMA_PATH = os.path.join(os.path.dirname(__file__), "..", "schema.sql")


class SchemaInitMiddleware(Middleware):
    """Middleware to automatically deploy dramatiq schema in DB, if not already present,
    during worker startup."""

    # Arbitrary ID used for postgres advisory locks
    LOCK_ID = 682834

    def after_process_boot(self, broker):
        with transaction(broker.pool) as cursor:
            cursor.execute("select pg_advisory_lock(%s)", (self.LOCK_ID,))

            cursor.execute(
                "select count(1) from information_schema.tables where table_schema=%s",
                ("dramatiq",),
            )
            have_dramatiq = cursor.fetchone()

            if have_dramatiq[0]:
                LOG.debug("dramatiq schema is already in place")
                return

            with open(SCHEMA_PATH, "rt") as f:
                schema = f.read()
            cursor.execute(schema)

            LOG.info("Applied dramatiq schema from %s", SCHEMA_PATH)
