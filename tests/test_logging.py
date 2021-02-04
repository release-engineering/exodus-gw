import logging

from exodus_gw.main import loggers_init
from exodus_gw.settings import load_settings


def test_log_levels():
    """Ensure loggers are configured according to exodus-gw.ini."""

    logging.getLogger("old-logger").setLevel("DEBUG")

    loggers_init(load_settings())

    # Should not alter existing loggers.
    assert logging.getLogger("old-logger").level == logging.DEBUG

    # Should set level of new loggers according to exodus-gw.ini.
    assert logging.getLogger().level == logging.INFO
    assert logging.getLogger("exodus-gw").level == logging.WARN
    assert logging.getLogger("s3").level == logging.DEBUG


def test_log_handler():
    """Ensure handler is added to root logger when none are present"""

    root_logger = logging.getLogger()
    root_handlers = root_logger.handlers

    # Clear existing handlers.
    root_handlers.clear()
    assert not root_handlers

    loggers_init(load_settings())

    # Should now have one (1) StreamHandler.
    assert len(root_handlers) == 1
    assert type(root_handlers[0]) == logging.StreamHandler
