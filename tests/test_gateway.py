import pytest

from exodus_gw.routers import gateway


def test_healthcheck():
    assert gateway.healthcheck() == {"detail": "exodus-gw is running"}


@pytest.mark.parametrize(
    "env,expected",
    [
        ("dev", {"detail": "Created Publish Id"}),
        ("qa", {"detail": "Created Publish Id"}),
        ("stage", {"detail": "Created Publish Id"},),
        ("prod", {"detail": "Created Publish Id"},),
        (
            "env_doesnt_exist",
            {"error": "environment env_doesnt_exist not found"},
        ),
    ],
)
def test_publish(env, expected):
    assert gateway.publish(env) == expected


def test_whoami():
    # All work is done by fastapi deserialization, so this doesn't actually
    # do anything except return the passed object.
    context = object()
    assert gateway.whoami(context=context) is context
