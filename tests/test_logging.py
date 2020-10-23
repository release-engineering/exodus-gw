import logging

from exodus_gw.app import configure_loggers


def test_log_levels():
    """Ensure loggers are configured according to exodus-gw.ini."""

    logging.getLogger("old-logger").setLevel("DEBUG")

    configure_loggers()

    # Should not alter existing loggers.
    assert logging.getLogger("old-logger").level == 10

    # Should set level of new loggers according to exodus-gw.ini.
    assert logging.getLogger().level == 20
    assert logging.getLogger("exodus-gw").level == 30
    assert logging.getLogger("s3").level == 10
