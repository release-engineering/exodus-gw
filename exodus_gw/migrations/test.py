"""Utilities to assist in testing migrations."""

import functools
import os
import typing


def tested_by(data_fn: typing.Callable[[], None]):
    """A decorator declaring a function which provides test data for
    an upgrade or downgrade operation.

    Usage:

    >>> def my_data():
    >>>    # alembic ops to populate some data
    >>>    op.execute(...)
    >>>
    >>> @tested_by(my_data)
    >>> def upgrade():
    >>>    # the usual alembic ops to do an upgrade

    When migrations are applied during test_migrations in this repo,
    the data function will run prior to the upgrade/downgrade.

    In all other contexts, the data function has no effect.

    Note that using this decorator does not by itself verify that
    any migrations of existing data work correctly, it only verifies
    that migration can complete without crashing.
    """

    def decorator(fn: typing.Callable[[], None]):
        @functools.wraps(fn)
        def fn_with_data():
            # Call the data function before the real migration function,
            # but only if this env var is set.
            if os.environ.get("EXODUS_GW_TESTING_MIGRATIONS") == "1":
                data_fn()
            fn()

        return fn_with_data

    return decorator
