import logging

from exodus_gw.dramatiq.broker import Broker


def test_broker_inits_loggers(tmpdir, monkeypatch):
    """Constructing a broker should set log levels according to config file."""

    # Set up a config file which assigns non-default log levels.
    ini = tmpdir.join("exodus-gw.ini")
    ini.write("[loglevels]\nmy-great-logger = DEBUG\n")
    monkeypatch.setenv("EXODUS_GW_INI_PATH", str(ini))

    # Initially, my logger shouldn't have any level set.
    assert logging.getLogger("my-great-logger").level == logging.NOTSET

    # But if I construct a broker...
    Broker()

    # That should force loading settings and configuring loggers,
    # so the level should now be what I requested
    assert logging.getLogger("my-great-logger").level == logging.DEBUG
