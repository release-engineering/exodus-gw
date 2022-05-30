import backoff
from dramatiq import Middleware
from sqlalchemy import inspect


@backoff.on_predicate(
    wait_gen=backoff.expo,
    max_tries=24,
    max_time=120,
)
def db_table_check(engine, table):
    return inspect(engine).has_table(table)


class DatabaseReadyMiddleware(Middleware):
    """Middleware for checking if DB is ready."""

    def __init__(self, db_engine):
        self.__db_engine = db_engine

    def after_process_boot(self, broker):
        db_table_check(self.__db_engine, "dramatiq_consumers")
