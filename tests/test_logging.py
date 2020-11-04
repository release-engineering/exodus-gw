import logging

from exodus_gw.main import configure_loggers


def test_log_levels():
    """Ensure loggers are configured according to exodus-gw.ini."""

    logging.getLogger("old-logger").setLevel("DEBUG")

    configure_loggers()

    # Should not alter existing loggers.
    assert logging.getLogger("old-logger").level == logging.DEBUG

    # Should set level of new loggers according to exodus-gw.ini.
    assert logging.getLogger().level == logging.INFO
    assert logging.getLogger("exodus-gw").level == logging.WARN
    assert logging.getLogger("s3").level == logging.DEBUG
