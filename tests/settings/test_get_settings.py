from exodus_gw.settings import get_settings


# Note: get_settings is wrapped in lru_cache.
# During tests, we want to test the real original function
# without caching, so we grab a reference to it here.
get_settings = get_settings.__wrapped__


def test_get_settings_default():
    """get_settings returns an object with default settings present."""

    settings = get_settings()

    assert settings.call_context_header == "X-RhApiPlatform-CallContext"


def test_get_settings_override(monkeypatch):
    """get_settings values can be overridden by environment variables.

    This test shows/proves that the pydantic BaseSettings environment variable
    parsing feature is generally working. It is not necessary to add similar
    tests for every value in settings.
    """

    monkeypatch.setenv("EXODUS_GW_CALL_CONTEXT_HEADER", "my-awesome-header")

    settings = get_settings()

    # It should have used the value from environment.
    assert settings.call_context_header == "my-awesome-header"
