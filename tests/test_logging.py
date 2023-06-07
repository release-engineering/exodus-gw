import logging

from freezegun import freeze_time

from exodus_gw.main import loggers_init
from exodus_gw.settings import load_settings


def test_log_levels():
    """Ensure loggers are configured according to exodus-gw.ini."""

    logging.getLogger().setLevel("INFO")
    logging.getLogger("old-logger").setLevel("DEBUG")

    loggers_init(load_settings())

    # Should not alter existing loggers.
    assert logging.getLogger("old-logger").level == logging.DEBUG

    # Should set level of new loggers according to exodus-gw.ini.
    assert logging.getLogger().level == logging.WARN
    assert logging.getLogger("exodus-gw").level == logging.INFO
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


@freeze_time("2023-04-26 14:43:13.570034+00:00")
def test_json_logger_configurable_datefmt(caplog):
    """Ensure logger's datefmt is configurable"""

    settings = load_settings()
    settings.log_config["datefmt"] = "%H:%M on %A, %B %d, %Y"

    loggers_init(settings)
    logging.getLogger("exodus-gw").info("...")

    # Logged timestamp should be formatted as configured in settings,
    # default: "2023-04-26 14:43:13.570"
    assert '"time": "14:43 on Wednesday, April 26, 2023"' in caplog.text


def test_json_logger_stack_info(caplog):
    loggers_init(load_settings())
    logging.getLogger("exodus-gw").exception("oops", stack_info=True)
    assert '"stack_info": "Stack (most recent call last)' in caplog.text
