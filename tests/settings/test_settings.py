import pytest
from fastapi import HTTPException

from exodus_gw.settings import get_environment, load_settings


def test_load_settings_default():
    """load_settings returns an object with default settings present."""

    settings = load_settings()

    assert settings.call_context_header == "X-RhApiPlatform-CallContext"
    assert [env.name for env in settings.environments] == [
        "test",
        "test2",
        "test3",
    ]
    assert settings.db_service_user == "exodus-gw"
    assert settings.db_service_pass == "exodus-gw"


def test_load_settings_override(monkeypatch):
    """load_settings values can be overridden by environment variables.

    This test shows/proves that the pydantic BaseSettings environment variable
    parsing feature is generally working. It is not necessary to add similar
    tests for every value in settings.
    """

    monkeypatch.setenv("EXODUS_GW_CALL_CONTEXT_HEADER", "my-awesome-header")

    settings = load_settings()

    # It should have used the value from environment.
    assert settings.call_context_header == "my-awesome-header"


@pytest.mark.parametrize(
    "env,expected",
    [
        (
            "test",
            {
                "aws_profile": "test",
                "bucket": "my-bucket",
                "table": "my-table",
            },
        ),
        (
            "test2",
            {
                "aws_profile": "test2",
                "bucket": "my-bucket2",
                "table": "my-table2",
            },
        ),
        (
            "test3",
            {
                "aws_profile": "test3",
                "bucket": "my-bucket3",
                "table": "my-table3",
            },
        ),
        ("bad", None),
    ],
    ids=["test", "test2", "test3", "bad"],
)
def test_get_environment(env, expected):
    if expected:
        env_obj = get_environment(env)

        assert env_obj.aws_profile == expected["aws_profile"]
        assert env_obj.bucket == expected["bucket"]
        assert env_obj.table == expected["table"]

    else:
        with pytest.raises(HTTPException) as exc_info:
            get_environment(env)

        assert exc_info.value.status_code == 404
        assert exc_info.value.detail == "Invalid environment='bad'"
