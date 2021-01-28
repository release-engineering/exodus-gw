import mock

from exodus_gw.routers import service


def test_healthcheck():
    assert service.healthcheck() == {"detail": "exodus-gw is running"}


def test_healthcheck_worker(stub_worker):
    # This test exercises "real" dramatiq message handling via stub
    # broker & worker, demonstrating that messages can be used from
    # within tests.
    assert service.healthcheck_worker(db=mock.Mock()) == {
        "detail": "background worker is running: ping => pong"
    }


def test_whoami():
    # All work is done by fastapi deserialization, so this doesn't actually
    # do anything except return the passed object.
    context = object()
    assert service.whoami(context=context) is context
